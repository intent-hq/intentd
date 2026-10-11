const fs = require('node:fs');

function writeOutput(data) {
  const bytes = Buffer.from(data);
  let offset = 0;
  while (offset < bytes.length) {
    offset += fs.writeSync(1, bytes, offset, bytes.length - offset);
  }
}

writeOutput('IO_READY\n');
const input = Buffer.alloc(4096);
if (fs.readSync(0, input, 0, 1, null) !== 1 || input[0] !== 0x78) {
  throw new Error('Expected terminal input before output burst');
}
writeOutput('OUTPUT_FLOW\n'.repeat(16384));

let lines = 0;
while (lines < 128) {
  const count = fs.readSync(0, input, 0, input.length, null);
  if (count === 0) throw new Error('Terminal input ended early');
  for (let index = 0; index < count; index += 1) {
    const byte = input[index];
    if (byte === 0x0a) lines += 1;
    else if (byte !== 0x78 && byte !== 0x0d) {
      throw new Error(`Unexpected terminal input byte: ${byte}`);
    }
  }
}
writeOutput('\nIO_INPUT_DONE\nIO_OUTPUT_DONE\n');
process.exit(0);
