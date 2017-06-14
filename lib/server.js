const http = require('http')
const url = require('url')
const _ = require('lodash')
const redis = require('../src/redis')

module.exports = http.createServer(requestHandler)

const redisClient = redis({port: 6379, host: '127.0.0.1'})

function requestHandler (request, response) {
  let body = []
  request.on('error', function (err) {
    console.error(err)
  }).on('data', function (chunk) {
    body.push(chunk)
  }).on('end', function () {
    body = Buffer.concat(body).toString()
    if (request.method === 'POST') {
      try {
        body = JSON.parse(body)
      } catch (e) {
        response.writeHead(500, {'Content-Type': 'applicatoin/json'})
        response.write(JSON.stringify({message: 'Error in parsing JSON'}))
        return response.end()
      }
    }
    if (request.method === 'POST' && request.url === '/buyers') {
      handleAddBuyerRequest(request, response, body)
    } else if (request.method === 'GET' &&
      (request.url.indexOf('/buyers') !== -1) &&
      (request.url.split('/buyers/').length === 2)) {
      handleGetBuyerRequest(request, response, body)
    } else if (request.method === 'GET' &&
      (request.url.indexOf('/route') !== -1) &&
      (request.url.split('/route?').length === 2)) {
      handleRouteBuyerRequest(request, response, body)
    } else {
      response.writeHead(404, {'Content-Type': 'application/json'})
      response.write(JSON.stringify({message: 'API not found'}))
      return response.end()
    }
  })
}

function handleAddBuyerRequest (request, response, body) {
  addBuyer(body, function (result) {
    response.writeHead(result.statusCode, {'Content-Type': 'application/json'})
    response.write(JSON.stringify({message: result.message}))
    return response.end()
  })
}

function handleGetBuyerRequest (request, response, body) {
  getBuyer(request.url.split('/buyers/')[1], function (err, buyer) {
    if (err) {
      response.writeHead(err.statusCode, {'Content-Type': 'application/json'})
      response.write(JSON.stringify({message: err.message}))
      return response.end()
    }
    if (typeof buyer === 'string') {
      buyer = JSON.parse(buyer)
      buyer.offers = _.map(buyer.offers, (offer) => _.omit(offer, 'id'))
      buyer = JSON.stringify(buyer)
    }
    if (typeof buyer === 'object') {
      buyer = JSON.stringify(buyer)
    }
    response.writeHead(200, {'Content-Type': 'application/json'})
    response.write(buyer)
    return response.end()
  })
}

function handleRouteBuyerRequest (request, response, body) {
  const queryData = url.parse(request.url, true).query
  findBuyer(queryData, function (err, offerLocation) {
    if (err) {
      response.writeHead(err.statusCode, {'Content-Type': 'application/json'})
      response.write(JSON.stringify({message: err.message}))
      return response.end()
    }
    if (typeof offerLocation === 'object') {
      offerLocation = JSON.stringify(offerLocation)
    }
    response.writeHead(302, {
      'Content-Type': 'application/json',
      'location': offerLocation
    })
    response.write(JSON.stringify({location: offerLocation}))
    return response.end()
  })
}
function addBuyer (body, cb) {
  if (!validateBuyer(body)) {
    const err = {statusCode: 400, message: 'Invalid buyer schema'}
    return cb(err)
  }

  body.offers.forEach((offer, index) => {
    offer.id = `offer:${body.id}:${index}`
  })
  const buyerKey = `buyers:${body.id}`
  setDetailsForBuyer(buyerKey, body)
  const result = {statusCode: 201, message: 'Buyer has been added'}
  return cb(result)

  function setDetailsForBuyer (key, buyer) {
    redisClient.set(key, JSON.stringify(buyer))
    const offers = buyer.offers
    offers.forEach((offer) => {
      const {device, hour, day, state} = offer.criteria
      device.forEach(i => redisClient.sadd(
        `device:${i}`,
        `buyers:${buyer.id}-${offer.id}`
      ))
      hour.forEach(i => redisClient.sadd(
        `hour:${i}`,
        `buyers:${buyer.id}-${offer.id}`
      ))
      day.forEach(i => redisClient.sadd(
        `day:${i}`,
        `buyers:${buyer.id}-${offer.id}`
      ))
      state.forEach(i => redisClient.sadd(
        `state:${i}`,
        `buyers:${buyer.id}-${offer.id}`
      ))
    })
  }

  function validateBuyer (buyer) {
    if (!buyer.id || !buyer.offers) {
      return false
    }
    if (!Array.isArray(buyer.offers)) {
      return false
    }

    return buyer.offers.every((offer) => {
      if (!offer.value || !offer.location) return false

      if (!offer.criteria) return false

      const {device, hour, day, state} = offer.criteria
      if (!Array.isArray(device) ||
         (!Array.isArray(hour)) ||
         (!Array.isArray(day)) ||
         (!Array.isArray(state))) {
        return false
      }
      return true
    })
  }
}

function getBuyer (id, cb) {
  redisClient.get(`buyers:${id}`, function (err, buyer) {
    if (err) return cb(err)

    if (!buyer) {
      const error = {statusCode: 404, message: 'No buyer found'}
      return cb(error)
    }
    return cb(null, buyer)
  })
}

function findBuyer (query, cb) {
  let {timestamp, device, state} = query
  const {day, hour} = parseISODate(timestamp)

  redisClient.sinter(
    `hour:${hour}`,
    `day:${day}`,
    `device:${device}`,
    `state:${state}`, function (err, data) {
      if (err) return cb(err)
      if (Array.isArray(data) && !data.length) {
        const error = {statusCode: 404, message: 'No buyer match found'}
        return cb(error)
      }

      let buyerIds = []
      let offerIds = []
      data.forEach((item) => {
        const [buyerId, offerId] = item.split('-')
        buyerIds.push(buyerId)
        offerIds.push(offerId)
      })

      redisClient.mget(buyerIds, function (err, data) {
        if (err) return cb(err)
        const buyers = data.map((buyer) => JSON.parse(buyer))
        const offers = _.flattenDeep(_.map(buyers, 'offers'))
        const matchedOffers = offers
          .filter(offer => offerIds.indexOf(offer.id) !== -1)
        return cb(null, _.maxBy(matchedOffers, 'value').location)
      })
    })

  function parseISODate (ISOString) {
    const date = new Date(ISOString)
    return {
      day: date.getUTCDay(),
      hour: date.getUTCHours()
    }
  }
}
