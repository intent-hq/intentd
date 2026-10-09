// Finite, local synthetic children only. Never imports an adapter or opens a socket.
import {spawn} from 'node:child_process';
import {writeSync,writeFileSync} from 'node:fs';
const [mode,marker,...rest]=process.argv.slice(2);
if(mode==='entry'){writeFileSync(marker,'entered',{flag:'wx'});writeSync(1,'entry\n');}
else if(mode==='nonzero'){writeSync(2,'synthetic exit\n');process.exitCode=7;}
else if(mode==='arguments'){if(JSON.stringify(rest)!==JSON.stringify(['space value','quote"value','tail\\','&()%^!']))throw Error('argument mismatch');writeSync(1,'arguments-ok\n');}
else if(mode==='wait'){setTimeout(()=>{},30000);}
else if(mode==='chain'){spawn(process.execPath,[import.meta.filename,'grandparent',marker],{stdio:'inherit'});setTimeout(()=>{},30000);}
else if(mode==='grandparent'){spawn(process.execPath,[import.meta.filename,'wait',marker],{stdio:'inherit'});setTimeout(()=>{},30000);}
else if(mode==='retired-parent'){spawn(process.execPath,[import.meta.filename,'wait',marker],{stdio:'inherit'});setTimeout(()=>process.exit(0),100);}
else if(mode==='flood'){const b=Buffer.alloc(4096,65);for(let i=0;i<10000;i++)writeSync(1,b);}
else throw Error('unselected synthetic mode');
