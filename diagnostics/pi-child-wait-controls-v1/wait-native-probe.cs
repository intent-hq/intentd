using System;
using System.Text;
using System.Runtime.InteropServices;
using System.Threading;
public static class WaitDiagnosticProbe {
 private static int safe=1;
 public static bool OwnershipSafe {get{return Volatile.Read(ref safe)==1;}}
 [StructLayout(LayoutKind.Sequential,CharSet=CharSet.Unicode)] struct SI {public int cb;public string reserved,desktop,title;public int x,y,xSize,ySize,xCount,yCount,fill,flags;public short show,reserved2;public IntPtr reservedBytes,input,output,error;}
 [StructLayout(LayoutKind.Sequential)] struct PI {public IntPtr process,thread;public uint pid,tid;}
 [StructLayout(LayoutKind.Sequential)] struct BASIC {public long processTime,jobTime;public uint flags;public UIntPtr min,max;public uint active;public UIntPtr affinity;public uint priority,scheduling;}
 [StructLayout(LayoutKind.Sequential)] struct IO {public ulong rOps,wOps,oOps,rBytes,wBytes,oBytes;}
 [StructLayout(LayoutKind.Sequential)] struct EXT {public BASIC basic;public IO io;public UIntPtr processMemory,jobMemory,peakProcess,peakJob;}
 [DllImport("kernel32.dll",SetLastError=true,CharSet=CharSet.Unicode)] static extern IntPtr CreateJobObject(IntPtr sa,string name);
 [DllImport("kernel32.dll",SetLastError=true)] static extern bool SetInformationJobObject(IntPtr j,int cls,ref EXT value,int length);
 [DllImport("kernel32.dll",SetLastError=true)] static extern bool AssignProcessToJobObject(IntPtr j,IntPtr p);
 [DllImport("kernel32.dll",SetLastError=true)] static extern bool IsProcessInJob(IntPtr p,IntPtr j,out bool inside);
 [DllImport("kernel32.dll",SetLastError=true)] static extern bool TerminateJobObject(IntPtr j,uint code);
 [DllImport("kernel32.dll",SetLastError=true)] static extern bool TerminateProcess(IntPtr p,uint code);
 [DllImport("kernel32.dll",SetLastError=true,CharSet=CharSet.Unicode)] static extern bool CreateProcess(string app,StringBuilder command,IntPtr psa,IntPtr tsa,bool inherit,uint flags,IntPtr env,string cwd,ref SI si,out PI pi);
 [DllImport("kernel32.dll",SetLastError=true)] static extern uint WaitForSingleObject(IntPtr p,uint ms);
 [DllImport("kernel32.dll",SetLastError=true)] static extern bool GetProcessTimes(IntPtr p,out long born,out long exited,out long kernel,out long user);
 [DllImport("kernel32.dll",SetLastError=true)] static extern uint GetProcessId(IntPtr p);
 [DllImport("kernel32.dll")] static extern bool CloseHandle(IntPtr p);
 public sealed class Receipt {
  public bool created,assigned,inJob,neverResumed=true,initialReturned,finalReturned,invalidReturned,identityStable,terminationAttempted,terminationSucceeded,processSignalled,processClosed,threadClosed,jobClosed,complete;
  public bool cachedErrorRestored;public uint initialWait,finalWait,invalidWait;public int initialError,finalError,invalidError;public string phase="initial";
 }
 static void Check(bool ok){if(!ok)throw new InvalidOperationException("probe_failure");}
 public static Receipt Run(string exe){
  if(!System.IO.Path.IsPathFullyQualified(exe)||!System.IO.File.Exists(exe)||exe.IndexOf('"')>=0)throw new ArgumentException("probe_input");
  if(Interlocked.CompareExchange(ref safe,0,1)!=1)throw new InvalidOperationException("probe_ownership_unresolved");
  int savedError=Marshal.GetLastPInvokeError();var r=new Receipt();IntPtr job=IntPtr.Zero;var pi=new PI();long birth=0;bool birthKnown=false;
  try {
   r.phase="job";job=CreateJobObject(IntPtr.Zero,null);Check(job!=IntPtr.Zero);var lim=new EXT();lim.basic.flags=0x2000|0x8;lim.basic.active=1;Check(SetInformationJobObject(job,9,ref lim,Marshal.SizeOf<EXT>()));
   r.phase="create-suspended";var si=new SI();si.cb=Marshal.SizeOf<SI>();Check(CreateProcess(exe,new StringBuilder("\""+exe+"\""),IntPtr.Zero,IntPtr.Zero,false,0x4|0x08000000,IntPtr.Zero,null,ref si,out pi));r.created=true;
   r.phase="assign";Check(AssignProcessToJobObject(job,pi.process));r.assigned=true;bool inside;Check(IsProcessInJob(pi.process,job,out inside)&&inside);r.inJob=true;
   long exited,kernel,user;birthKnown=GetProcessTimes(pi.process,out birth,out exited,out kernel,out user);Check(birthKnown&&GetProcessId(pi.process)==pi.pid);
   r.phase="initial-wait";r.initialWait=WaitForSingleObject(pi.process,0);r.initialError=r.initialWait==uint.MaxValue?Marshal.GetLastWin32Error():0;r.initialReturned=true;Check(r.initialWait==258);
   r.phase="terminate-owned-job";r.terminationAttempted=true;r.terminationSucceeded=TerminateJobObject(job,125);Check(r.terminationSucceeded);
   r.phase="final-wait";r.finalWait=WaitForSingleObject(pi.process,1000);r.finalError=r.finalWait==uint.MaxValue?Marshal.GetLastWin32Error():0;r.finalReturned=true;r.processSignalled=r.finalWait==0;Check(r.processSignalled);
   long after;r.identityStable=GetProcessTimes(pi.process,out after,out exited,out kernel,out user)&&after==birth&&GetProcessId(pi.process)==pi.pid;Check(r.identityStable);
   // NULL is explicitly invalid, not a borrowed/open process handle. Never close or signal it.
   r.phase="invalid-wait";r.invalidWait=WaitForSingleObject(IntPtr.Zero,0);r.invalidError=r.invalidWait==uint.MaxValue?Marshal.GetLastWin32Error():0;r.invalidReturned=true;Check(r.invalidWait==uint.MaxValue&&r.invalidError==6);
   r.phase="complete";r.complete=true;
  }catch {r.complete=false;}
  finally {
   if(pi.process!=IntPtr.Zero&&!r.terminationAttempted){r.terminationAttempted=true;r.terminationSucceeded=r.assigned?TerminateJobObject(job,125):TerminateProcess(pi.process,125);}
   if(pi.process!=IntPtr.Zero&&!r.processSignalled)r.processSignalled=WaitForSingleObject(pi.process,1000)==0;
   if(pi.thread!=IntPtr.Zero)r.threadClosed=CloseHandle(pi.thread);else r.threadClosed=true;
   if(pi.process!=IntPtr.Zero)r.processClosed=CloseHandle(pi.process);else r.processClosed=true;
   if(job!=IntPtr.Zero)r.jobClosed=CloseHandle(job);else r.jobClosed=true;
   if((!r.created||r.processSignalled)&&r.processClosed&&r.threadClosed&&r.jobClosed)Interlocked.Exchange(ref safe,1);
   if(!OwnershipSafe)r.complete=false;
   Marshal.SetLastPInvokeError(savedError);r.cachedErrorRestored=Marshal.GetLastPInvokeError()==savedError;
  }
  return r;
 }
}
