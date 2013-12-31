//introduce assert as a global
assert = require('assert')
var bufferEqual = require('buffer-equal')

assert.bufferEqual = function(actual, expected) {
  if(!bufferEqual(actual, expected)) {
    console.log('')
    console.log('actual:   ', actual)
    console.log('expected: ', expected)
  }
  assert(bufferEqual(actual, expected))
}
