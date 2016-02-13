require('babel/register', {
  stage: 0
})

const path = process.argv[2] || 'metadata'
require('./' + path)
