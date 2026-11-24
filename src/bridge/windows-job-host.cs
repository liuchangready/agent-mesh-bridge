
using System;
using System.ComponentModel;
using System.Globalization;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;

internal static class WindowsJob
{
    private const uint KillOnClose = 0x2000;
    private const uint NoWindow = 0x08000000;
    private const uint ExtendedStartup = 0x00080000;
    private const uint WaitTimeout = 258;
    private const uint OwnerAccess = 0x00101000; // SYNCHRONIZE | PROCESS_QUERY_LIMITED_INFORMATION

    [StructLayout(LayoutKind.Sequential)]
    private struct BasicLimits
    {
        internal long ProcessTime, JobTime;
        internal uint Flags;
        internal UIntPtr MinimumWorkingSet, MaximumWorkingSet;
        internal uint ActiveLimit;
        internal UIntPtr Affinity;
        internal uint Priority, Scheduling;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct ExtendedLimits
    {
        internal BasicLimits Basic;
        internal ulong ReadOps, WriteOps, OtherOps, ReadBytes, WriteBytes, OtherBytes;
        internal UIntPtr ProcessMemory, JobMemory, PeakProcessMemory, PeakJobMemory;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct Accounting
    {
        internal long User, Kernel, PeriodUser, PeriodKernel;
        internal uint Faults, Total, Active, Terminated;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct Startup
    {
        internal uint Size;
        internal IntPtr Reserved, Desktop, Title;
        internal uint X, Y, XSize, YSize, XCount, YCount, Fill, Flags;
        internal ushort Show, ReservedBytes;
        internal IntPtr ReservedData, Input, Output, Error, Attributes;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct Child
    {
        internal IntPtr Process, Thread;
        internal uint Pid, Tid;
    }

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern IntPtr CreateJobObjectW(IntPtr attributes, IntPtr name);
    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool SetInformationJobObject(IntPtr job, int kind, ref ExtendedLimits info, int size);
    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool QueryInformationJobObject(IntPtr job, int kind, out Accounting info, int size, IntPtr returned);
    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool TerminateJobObject(IntPtr job, uint exitCode);
    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern IntPtr OpenProcess(uint access, bool inherit, uint pid);
    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool GetProcessTimes(IntPtr process, out long created, out long exited, out long kernel, out long user);
    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern uint WaitForSingleObject(IntPtr handle, uint milliseconds);
    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool GetExitCodeProcess(IntPtr process, out uint code);
    [DllImport("kernel32.dll")]
    private static extern bool CloseHandle(IntPtr handle);
    [DllImport("kernel32.dll")]
    private static extern IntPtr GetCurrentProcess();
    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern IntPtr GetStdHandle(int kind);
    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool DuplicateHandle(IntPtr sourceProcess, IntPtr source, IntPtr targetProcess,
        out IntPtr target, uint access, bool inherit, uint flags);
    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool InitializeProcThreadAttributeList(IntPtr list, int count, uint flags, ref IntPtr bytes);
    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool UpdateProcThreadAttribute(IntPtr list, uint flags, IntPtr kind, IntPtr data,
        IntPtr bytes, IntPtr previous, IntPtr returned);
    [DllImport("kernel32.dll")]
    private static extern void DeleteProcThreadAttributeList(IntPtr list);
    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    private static extern bool CreateProcessW(string application, StringBuilder commandLine, IntPtr processAttributes,
        IntPtr threadAttributes, bool inherit, uint flags, IntPtr environment, string directory,
        ref Startup startup, out Child child);

    private static void Check(bool ok, string operation)
    {
        if (!ok) throw new Win32Exception(Marshal.GetLastWin32Error(), operation);
    }

    private static bool Exited(IntPtr process, uint wait)
    {
        uint state = WaitForSingleObject(process, wait);
        Check(state == 0 || state == WaitTimeout, "WaitForSingleObject");
        return state == 0;
    }

    private static IntPtr InheritStream(int stream)
    {
        IntPtr self = GetCurrentProcess();
        IntPtr copy;
        Check(DuplicateHandle(self, GetStdHandle(stream), self, out copy, 0, true, 2), "Duplicate standard stream");
        return copy;
    }

    private static string Quote(string arg)
    {
        var text = new StringBuilder("\"");
        int backslashes = 0;
        foreach (char ch in arg)
        {
            if (ch == '\\') { backslashes++; continue; }
            text.Append('\\', ch == '"' ? 2 * backslashes + 1 : backslashes);
            text.Append(ch);
            backslashes = 0;
        }
        return text.Append('\\', 2 * backslashes).Append('"').ToString();
    }

    private static Child Launch(IntPtr job, string[] args)
    {
        var info = new Startup { Size = (uint)Marshal.SizeOf(typeof(Startup)), Flags = 0x100 };
        IntPtr bytes = IntPtr.Zero;
        IntPtr jobAttribute = IntPtr.Zero;
        IntPtr streams = IntPtr.Zero;
        bool initialized = false;
        try
        {
            info.Input = InheritStream(-10);
            info.Output = InheritStream(-11);
            info.Error = InheritStream(-12);
            InitializeProcThreadAttributeList(IntPtr.Zero, 2, 0, ref bytes);
            info.Attributes = Marshal.AllocHGlobal(bytes);
            Check(InitializeProcThreadAttributeList(info.Attributes, 2, 0, ref bytes), "Initialize process attributes");
            initialized = true;
            jobAttribute = Marshal.AllocHGlobal(IntPtr.Size);
            Marshal.WriteIntPtr(jobAttribute, job);
            Check(UpdateProcThreadAttribute(info.Attributes, 0, (IntPtr)0x2000d, jobAttribute,
                (IntPtr)IntPtr.Size, IntPtr.Zero, IntPtr.Zero), "Set creation-time job membership");
            streams = Marshal.AllocHGlobal(IntPtr.Size * 3);
            Marshal.WriteIntPtr(streams, 0, info.Input);
            Marshal.WriteIntPtr(streams, IntPtr.Size, info.Output);
            Marshal.WriteIntPtr(streams, IntPtr.Size * 2, info.Error);
            Check(UpdateProcThreadAttribute(info.Attributes, 0, (IntPtr)0x20002, streams,
                (IntPtr)(IntPtr.Size * 3), IntPtr.Zero, IntPtr.Zero), "Restrict inherited standard streams");

            string line;
            if (args[3] == "cmd")
            {
                string shell = Environment.GetEnvironmentVariable("ComSpec") ?? "cmd.exe";
                line = Quote(shell) + " /d /s /c \"" + string.Join(" ", args, 4, args.Length - 4) + "\"";
            }
            else
            {
                var fragments = new string[args.Length - 4];
                for (int i = 4; i < args.Length; i++) fragments[i - 4] = Quote(args[i]);
                line = string.Join(" ", fragments);
            }
            Child child;
            Check(CreateProcessW(null, new StringBuilder(line), IntPtr.Zero, IntPtr.Zero, true,
                ExtendedStartup | NoWindow, IntPtr.Zero, null, ref info, out child), "Create process in Job Object");
            return child;
        }
        finally
        {
            if (initialized) DeleteProcThreadAttributeList(info.Attributes);
            if (info.Attributes != IntPtr.Zero) Marshal.FreeHGlobal(info.Attributes);
            if (jobAttribute != IntPtr.Zero) Marshal.FreeHGlobal(jobAttribute);
            if (streams != IntPtr.Zero) Marshal.FreeHGlobal(streams);
            if (info.Input != IntPtr.Zero) CloseHandle(info.Input);
            if (info.Output != IntPtr.Zero) CloseHandle(info.Output);
            if (info.Error != IntPtr.Zero) CloseHandle(info.Error);
        }
    }

    private static uint ActiveProcesses(IntPtr job)
    {
        Accounting accounting;
        Check(QueryInformationJobObject(job, 1, out accounting, Marshal.SizeOf(typeof(Accounting)), IntPtr.Zero),
            "Read Job Object active processes");
        return accounting.Active;
    }

    private static void Complete(string prefix)
    {
        string temporary = prefix + ".tmp";
        using (var receipt = new FileStream(temporary, FileMode.CreateNew, FileAccess.Write, FileShare.None))
        {
            receipt.WriteByte(49);
            receipt.Flush(true);
        }
        File.Move(temporary, prefix + ".done");
    }

    private static int Main(string[] args)
    {
        if (args.Length < 5) return 125;
        string prefix = args[0];
        IntPtr owner = IntPtr.Zero;
        IntPtr job = IntPtr.Zero;
        Child child = new Child();
        try
        {
            owner = OpenProcess(OwnerAccess, false, uint.Parse(args[1], CultureInfo.InvariantCulture));
            if (owner == IntPtr.Zero && Marshal.GetLastWin32Error() == 87) { Complete(prefix); return 125; }
            Check(owner != IntPtr.Zero, "Open Bridge process");
            long created, unused1, unused2, unused3;
            Check(GetProcessTimes(owner, out created, out unused1, out unused2, out unused3), "Read Bridge process identity");
            if (created.ToString(CultureInfo.InvariantCulture) != args[2]) { Complete(prefix); return 125; }

            job = CreateJobObjectW(IntPtr.Zero, IntPtr.Zero);
            Check(job != IntPtr.Zero, "Create Job Object");
            var limits = new ExtendedLimits { Basic = new BasicLimits { Flags = KillOnClose } };
            Check(SetInformationJobObject(job, 9, ref limits, Marshal.SizeOf(typeof(ExtendedLimits))), "Set Job Object limits");

            // The business process starts only after the Bridge has bound this launcher to the run.
            var admission = System.Diagnostics.Stopwatch.StartNew();
            while (!File.Exists(prefix + ".start"))
            {
                if (File.Exists(prefix + ".stop") || Exited(owner, 20) || admission.ElapsedMilliseconds >= 120000)
                {
                    Complete(prefix);
                    return 125;
                }
            }
            if (File.Exists(prefix + ".stop") || Exited(owner, 0)) { Complete(prefix); return 125; }
            child = Launch(job, args);
            CloseHandle(child.Thread);
            bool cancelled = false;
            while (!Exited(child.Process, 20))
            {
                if (File.Exists(prefix + ".stop") || Exited(owner, 0)) { cancelled = true; break; }
            }

            // A normal CLI exit can leave background tasks behind too.
            if (cancelled || ActiveProcesses(job) != 0)
                Check(TerminateJobObject(job, 125), "Stop Job Object processes");
            if (!Exited(child.Process, 10000)) throw new InvalidOperationException("Job root exit timed out");
            var watch = System.Diagnostics.Stopwatch.StartNew();
            while (ActiveProcesses(job) != 0)
            {
                if (watch.ElapsedMilliseconds >= 10000) throw new InvalidOperationException("Job descendants did not exit");
                Thread.Sleep(20);
            }
            uint exitCode;
            Check(GetExitCodeProcess(child.Process, out exitCode), "Read child exit code");
            Complete(prefix);
            return cancelled ? 125 : unchecked((int)exitCode);
        }
        catch (Exception error)
        {
            Console.Error.WriteLine("Bridge Windows Job: " + error.Message);
            if (child.Process == IntPtr.Zero)
            {
                try { if (job == IntPtr.Zero || ActiveProcesses(job) == 0) Complete(prefix); } catch (Exception) { }
            }
            return 125;
        }
        finally
        {
            if (child.Process != IntPtr.Zero) CloseHandle(child.Process);
            if (job != IntPtr.Zero) CloseHandle(job);
            if (owner != IntPtr.Zero) CloseHandle(owner);
        }
    }
}
