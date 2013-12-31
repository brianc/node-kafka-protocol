var writeBytes = function(buffer, offset, val) {
  if(val === null) {
    buffer.writeInt32BE(-1, offset)
    offset += 4
  } else {
    buffer.writeInt32BE(val.length, offset)
    offset += 4
    if(val.length) {
      //TODO use buffer.copy? - make sure its fast
      var i = 0;
      for(; i < val.length; i++) {
        buffer[i + offset] = val[i]
      }
      offset += i
    }
  }
  return offset
}

module.exports = {
  writeBytes: writeBytes
}
