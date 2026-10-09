// Artifact-only Windows setup launcher; uncompiled/unexecuted pending review.
using System;
using System.IO;
using System.Text;
using System.Linq;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Win32.SafeHandles;
public static class OwnedSetup {
 public sealed class OwnershipLatch {
  private int ready=1;
  public bool Safe { get { return System.Threading.Volatile.Read(ref ready)==1; } }
  public void Begin() { if(Interlocked.CompareExchange(ref ready,0,1)!=1)throw new InvalidOperationException("ownership_unresolved"); }
  public void Complete(Receipt r,bool processCreated,bool readerStarted) {
   bool safe=(!processCreated&&!readerStarted) ||
    (processCreated&&!r.assigned&&!r.resumed&&r.rootSignalled&&!readerStarted) ||
    (r.assigned&&r.activeZero&&r.rootSignalled&&r.readerDone&&!r.readerError);
   if(safe)Interlocked.Exchange(ref ready,1);
  }
  public void Fault() { Interlocked.Exchange(ref ready,0); }
 }
 private static readonly OwnershipLatch Gate=new OwnershipLatch();
 public static bool OwnershipSafe { get { return Gate.Safe; } }
 [StructLayout(LayoutKind.Sequential)] struct SA { public int length; public IntPtr descriptor; public int inherit; }
 [StructLayout(LayoutKind.Sequential, CharSet=CharSet.Unicode)] struct SI { public int cb; public string reserved, desktop, title; public int x,y,xSize,ySize,xCount,yCount,fill,flags; public short show,reserved2; public IntPtr reservedBytes,input,output,error; }
 [StructLayout(LayoutKind.Sequential)] struct SIX { public SI si; public IntPtr attributes; }
 [StructLayout(LayoutKind.Sequential)] struct PI { public IntPtr process,thread; public uint pid,tid; }
 [StructLayout(LayoutKind.Sequential)] struct BASIC { public long processTime,jobTime; public uint flags; public UIntPtr min,max; public uint active; public UIntPtr affinity; public uint priority,scheduling; }
 [StructLayout(LayoutKind.Sequential)] struct IO { public ulong rOps,wOps,oOps,rBytes,wBytes,oBytes; }
 [StructLayout(LayoutKind.Sequential)] struct EXT { public BASIC basic; public IO io; public UIntPtr processMemory,jobMemory,peakProcess,peakJob; }
 [DllImport("kernel32.dll",SetLastError=true,CharSet=CharSet.Unicode)] static extern IntPtr CreateJobObject(IntPtr sa,string name);
 [DllImport("kernel32.dll",SetLastError=true)] static extern bool SetInformationJobObject(IntPtr j,int cls,ref EXT value,int length);
 [DllImport("kernel32.dll",SetLastError=true)] static extern bool QueryInformationJobObject(IntPtr j,int cls,IntPtr value,uint length,out uint needed);
 [DllImport("kernel32.dll",SetLastError=true)] static extern bool AssignProcessToJobObject(IntPtr j,IntPtr process);
 [DllImport("kernel32.dll",SetLastError=true)] static extern bool TerminateJobObject(IntPtr j,uint code);
 [DllImport("kernel32.dll",SetLastError=true)] static extern bool TerminateProcess(IntPtr p,uint code);
 [DllImport("kernel32.dll",SetLastError=true)] static extern bool CreatePipe(out IntPtr read,out IntPtr write,ref SA sa,uint size);
 [DllImport("kernel32.dll",SetLastError=true)] static extern bool SetHandleInformation(IntPtr h,uint mask,uint flags);
 [DllImport("kernel32.dll",SetLastError=true,CharSet=CharSet.Unicode)] static extern IntPtr CreateFile(string name,uint access,uint share,ref SA sa,uint creation,uint flags,IntPtr template);
 [DllImport("kernel32.dll",SetLastError=true)] static extern bool InitializeProcThreadAttributeList(IntPtr list,int count,int flags,ref IntPtr size);
 [DllImport("kernel32.dll",SetLastError=true)] static extern bool UpdateProcThreadAttribute(IntPtr list,uint flags,IntPtr key,IntPtr value,IntPtr size,IntPtr previous,IntPtr returned);
 [DllImport("kernel32.dll")] static extern void DeleteProcThreadAttributeList(IntPtr list);
 [DllImport("kernel32.dll",SetLastError=true,CharSet=CharSet.Unicode)] static extern bool CreateProcess(string app,StringBuilder command,IntPtr psa,IntPtr tsa,bool inherit,uint flags,IntPtr env,string cwd,ref SIX info,out PI pi);
 [DllImport("kernel32.dll",SetLastError=true)] static extern uint ResumeThread(IntPtr thread);
 [DllImport("kernel32.dll",SetLastError=true)] static extern uint WaitForSingleObject(IntPtr h,uint ms);
 [DllImport("kernel32.dll",SetLastError=true)] static extern bool GetExitCodeProcess(IntPtr h,out uint code);
 [DllImport("kernel32.dll",SetLastError=true)] static extern bool GetProcessTimes(IntPtr p,out long created,out long exited,out long kernel,out long user);
 [DllImport("kernel32.dll")] static extern bool CloseHandle(IntPtr h);
 public sealed class Receipt {
  public string stage="initial",reason="none"; public int win32=0; public uint rootPid,originalExit=259;
  public string rootCreation="0"; public long written,read; public volatile bool assigned,resumed,overflow,readerError,activeZero,rootSignalled,readerDone;
  public int maximumActive,snapshots; public long elapsedMs; public List<uint> observedMembers=new List<uint>();
  public List<string> interventions=new List<string>();
 }
 static void Check(bool ok,string stage) { if(!ok) throw new InvalidOperationException(stage+":"+Marshal.GetLastWin32Error()); }
 static string Quote(string s) { var b=new StringBuilder("\"");int n=0;foreach(char c in s){if(c=='\\'){n++;continue;}if(c=='\"'){b.Append('\\',n*2+1);b.Append(c);}else{b.Append('\\',n);b.Append(c);}n=0;}b.Append('\\',n*2);return b.Append('"').ToString(); }
 static uint[] Members(IntPtr job) { int size=8+IntPtr.Size*256;IntPtr b=Marshal.AllocHGlobal(size);try { uint need;Check(QueryInformationJobObject(job,3,b,(uint)size,out need),"query_job");int n=Marshal.ReadInt32(b,4);Check(n>=0&&n<=256&&Marshal.ReadInt32(b,0)==n,"job_members_truncated");var ids=new uint[n];for(int i=0;i<n;i++)ids[i]=checked((uint)Marshal.ReadIntPtr(b,8+i*IntPtr.Size).ToInt64());return ids;}finally{Marshal.FreeHGlobal(b);} }
 // Only trusted setup callers supply executable/args. No shell, user-configurable command, or behavioral fixture entry.
 public static Receipt Run(string exe,string[] args,string cwd,string log,int milliseconds,int cap,bool rejectAssignment) {
  if(milliseconds<100||milliseconds>120000||cap<64||cap>8388608||!Path.IsPathFullyQualified(exe)||!File.Exists(exe)||File.Exists(log))throw new ArgumentException("setup_bounds");
  Gate.Begin();var r=new Receipt();var watch=Stopwatch.StartNew();IntPtr job=IntPtr.Zero,rd=IntPtr.Zero,wr=IntPtr.Zero,input=IntPtr.Zero,attr=IntPtr.Zero,handles=IntPtr.Zero,env=IntPtr.Zero;PI pi=new PI();bool attrReady=false;Task reader=null;FileStream inputStream=null;FileStream outputStream=null;
  try {
   r.stage="job";job=CreateJobObject(IntPtr.Zero,null);Check(job!=IntPtr.Zero,"create_job");var lim=new EXT();lim.basic.flags=0x2000|0x8;lim.basic.active=64;Check(SetInformationJobObject(job,9,ref lim,Marshal.SizeOf<EXT>()),"set_job_limits");
   var sa=new SA{length=Marshal.SizeOf<SA>(),inherit=1};Check(CreatePipe(out rd,out wr,ref sa,4096),"pipe");Check(SetHandleInformation(rd,1,0),"read_not_inherited");input=CreateFile("NUL",0x80000000,3,ref sa,3,0,IntPtr.Zero);Check(input!=new IntPtr(-1),"null_input");
   IntPtr bytes=IntPtr.Zero;InitializeProcThreadAttributeList(IntPtr.Zero,1,0,ref bytes);Check(bytes.ToInt64()>0&&bytes.ToInt64()<65536,"attribute_size");attr=Marshal.AllocHGlobal(bytes);Check(InitializeProcThreadAttributeList(attr,1,0,ref bytes),"attribute_init");attrReady=true;
   handles=Marshal.AllocHGlobal(IntPtr.Size*2);Marshal.WriteIntPtr(handles,0,input);Marshal.WriteIntPtr(handles,IntPtr.Size,wr);Check(UpdateProcThreadAttribute(attr,0,new IntPtr(0x20002),handles,new IntPtr(IntPtr.Size*2),IntPtr.Zero,IntPtr.Zero),"handle_allowlist");
   var si=new SIX();si.si.cb=Marshal.SizeOf<SIX>();si.si.flags=0x100;si.si.input=input;si.si.output=wr;si.si.error=wr;si.attributes=attr;
   var vars=new SortedDictionary<string,string>(StringComparer.OrdinalIgnoreCase);foreach(string key in new[]{"PATH","SystemRoot","WINDIR","COMSPEC","PATHEXT","TEMP","TMP"}){string val=Environment.GetEnvironmentVariable(key);if(val!=null)vars[key]=val;}
   vars["USERPROFILE"]=cwd;vars["APPDATA"]=Path.Combine(cwd,"appdata");vars["LOCALAPPDATA"]=Path.Combine(cwd,"localappdata");vars["CI"]="true";
   env=Marshal.StringToHGlobalUni(string.Join("\0",vars.Select(x=>x.Key+"="+x.Value))+"\0\0");
   string command=string.Join(" ",new[]{exe}.Concat(args).Select(Quote));r.stage="create_suspended";Check(CreateProcess(exe,new StringBuilder(command),IntPtr.Zero,IntPtr.Zero,true,0x4|0x400|0x80000|0x08000000,env,cwd,ref si,out pi),"create_suspended");r.rootPid=pi.pid;long created,ex,k,u;Check(GetProcessTimes(pi.process,out created,out ex,out k,out u),"root_birth");r.rootCreation=created.ToString(System.Globalization.CultureInfo.InvariantCulture);
   r.stage="assign";bool assigned=AssignProcessToJobObject(rejectAssignment?IntPtr.Zero:job,pi.process);if(!assigned){r.win32=Marshal.GetLastWin32Error();r.reason="assignment_failed";return r;}r.assigned=true;
   CloseHandle(wr);wr=IntPtr.Zero;CloseHandle(input);input=IntPtr.Zero;
   inputStream=new FileStream(new SafeFileHandle(rd,true),FileAccess.Read,4096,false);rd=IntPtr.Zero;outputStream=new FileStream(log,FileMode.CreateNew,FileAccess.Write,FileShare.Read);
   reader=Task.Run(()=>{try{byte[] buffer=new byte[4096];int n;while((n=inputStream.Read(buffer,0,buffer.Length))>0){Interlocked.Add(ref r.read,n);int allowed=(int)Math.Min(n,Math.Max(0,cap-Interlocked.Read(ref r.written)));if(allowed>0){outputStream.Write(buffer,0,allowed);Interlocked.Add(ref r.written,allowed);}if(allowed<n){r.overflow=true;break;}}}catch{r.readerError=true;}});
   r.stage="resume";Check(ResumeThread(pi.thread)!=uint.MaxValue,"resume");r.resumed=true;r.stage="running";
   while(true){var ids=Members(job);r.snapshots++;r.maximumActive=Math.Max(r.maximumActive,ids.Length);foreach(uint id in ids)if(!r.observedMembers.Contains(id)&&r.observedMembers.Count<256)r.observedMembers.Add(id);
    if(ids.Length==0){r.activeZero=true;r.reason=r.overflow?"output_cap":r.readerError?"reader_error":"completed";break;}
    if(r.overflow){r.reason="output_cap";break;}if(r.readerError){r.reason="reader_error";break;}if(watch.ElapsedMilliseconds>=milliseconds){r.reason="deadline";break;}Thread.Sleep(10);
   }
   r.stage="disposal";
  }catch(Exception error){r.reason="setup_or_observation_error";r.win32=Marshal.GetLastWin32Error();r.stage=error is ArgumentException?"argument":"native_or_io";}
  finally {
   // Handles and an exclusive job, never numeric-PID termination.
   if(pi.process!=IntPtr.Zero&&!r.assigned){r.interventions.Add("terminate_unresumed_process_handle");if(!TerminateProcess(pi.process,125))r.win32=Marshal.GetLastWin32Error();}
   if(r.assigned&&!r.activeZero){r.interventions.Add("terminate_owned_job");if(!TerminateJobObject(job,125))r.win32=Marshal.GetLastWin32Error();var cleanup=Stopwatch.StartNew();while(cleanup.ElapsedMilliseconds<10000){try{if(Members(job).Length==0){r.activeZero=true;break;}}catch{break;}Thread.Sleep(10);}}
   if(pi.process!=IntPtr.Zero){r.rootSignalled=WaitForSingleObject(pi.process,1000)==0;uint code;if(GetExitCodeProcess(pi.process,out code))r.originalExit=code;}
   if(wr!=IntPtr.Zero)CloseHandle(wr);if(input!=IntPtr.Zero&&input!=new IntPtr(-1))CloseHandle(input);
   if(reader!=null){try{r.readerDone=reader.Wait(1000);}catch{r.readerError=true;}}
   if(!r.readerDone&&reader!=null)r.readerError=true;
   if(r.reason=="completed"&&r.overflow)r.reason="output_cap";if(r.reason=="completed"&&r.readerError)r.reason="reader_error";
   if(r.readerDone){inputStream?.Dispose();outputStream?.Dispose();} // Do not race a pending reader; process teardown remains fallback.
   if(rd!=IntPtr.Zero)CloseHandle(rd);if(pi.thread!=IntPtr.Zero)CloseHandle(pi.thread);if(pi.process!=IntPtr.Zero)CloseHandle(pi.process);if(job!=IntPtr.Zero)CloseHandle(job);
   if(attrReady)DeleteProcThreadAttributeList(attr);if(attr!=IntPtr.Zero)Marshal.FreeHGlobal(attr);if(handles!=IntPtr.Zero)Marshal.FreeHGlobal(handles);if(env!=IntPtr.Zero)Marshal.FreeHGlobal(env);r.elapsedMs=watch.ElapsedMilliseconds;Gate.Complete(r,pi.process!=IntPtr.Zero,reader!=null);
  }
  return r;
 }
}
