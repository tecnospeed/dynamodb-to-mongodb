const moment = require('moment')
const CONFIG = require('./config')
const mongo = require('mongodb-single-client')
const async = require('async')

const AWS = require('aws-sdk')

let doc = new AWS.DynamoDB.DocumentClient()

moment.locale('pt-br')

const modifyId = (data, callback) => {
  // caso necessário alterar o id
  let newArray = data.map(item => item)

  return callback(null, newArray)
}

const findAllDynamoDB = (table, callback) => {
  const params = { TableName: table }

  let items = []
  const executeFindDynamoDB = callback => {
    doc.scan(params, (err, result) => {
      if (err) return callback(err)

      items = items.concat(result.Items)

      if (!result.LastEvaluatedKey) return callback(err, items)

      params.ExclusiveStartKey = result.LastEvaluatedKey

      return executeFindDynamoDB(callback)
    })
  }

  return executeFindDynamoDB(callback)
}

const migrateMongoDB = callback =>
  async.each(
    CONFIG.collections,
    (collection, callback) => {
      findAllDynamoDB(collection.dynamo, (err, data) => {
        if (err) callback(err)
        modifyId(data, (err, _data) => {
          mongo(collection.mongo).insertMany(_data, (err, result) => {
            if (err) callback(err)
            console.log(
              '\n\tFinalizado importação da collection',
              collection.mongo
            )
            console.log('\t\tQuantidade de registros: ', _data.length)
            callback(null)
          })
        })
      })
    },
    err => {
      callback(null)
    }
  )

const run = () => {
  console.log(
    `\n Iniciando processo de importação ${moment().format(
      'L'
    )} - ${moment().format('LTS')}`
  )

  mongo.connect((err, result) => {
    if (err) throw err

    migrateMongoDB((err, data) => {
      if (err) throw err

      console.log(
        `\nFinalizando processo de importação ${moment().format(
          'L'
        )} - ${moment().format('LTS')}`
      )

      process.exit(0)
    })
  })
}

run()
