const crypto = require('crypto')
const { resolve } = require('path')


// 48 ->64位 yHE9Jux1bVtCfmfUbgAUhjr6s_OJpFl1FwwGauXOWlAa7qIhIzfpMypMxMsQnYAX
// 16,18 -> 24位字符: 43K1zmgDiTC_UHaey6CoCg== 18最后不会是2个=
// 6,8 -> 8B, 12B
exports.generateRandom = (len = 18) =>
  crypto.randomBytes(len)
    .toString('base64')
    .replace(/\//g, '_')
    .replace(/\+/g, '-')

exports.sha256 = (str, key, encoding = 'utf8', outCoding = 'hex') =>
  crypto.createHmac('sha256', key)
    .update(str, encoding)
    .digest(outCoding)

exports.md5 = (str, encoding = 'utf8') =>
  crypto.createHash('md5')
    .update(str, encoding)
    .digest('hex')

/**
 * hash 校验
 * @param {*} pubKey        pem格式，要有换行
 * @param {*} signature     base64 格式
 * @param {*} signStr       utf8 字符串
 * @param {*} signatureCode 'base64'
 * @param {*} hashes        'RSA-MD5' 'RSA-SHA256'
 */
exports.rsaVerify = (pubKey, signature, signStr, signatureCode = 'base64', hashes = 'RSA-MD5') =>
  crypto.createVerify(hashes)
    .update(signStr)
    .verify(pubKey, signature, signatureCode)
/* 
{
  var verify = crypto.createVerify(hashes)
  verify.update(signStr)
  return verify.verify(pubKey, signature, signatureCode)
} */

// 从对象中 指定属性 生成新对象 'prop1 prop2'
exports.selectProp = (obj = {}, seleStr = '') => {
  let include = null
  if (seleStr.includes('-')) {
    const exclude = seleStr.replace('-', '').split(' ')
    include = Object.keys(obj).filter(i => !exclude.includes(i))
  } else include = seleStr.split(' ')
  return include.reduce((o, key) => ({ ...o, [key]: obj[key] }), {})
}

exports.selectBody = (body, seleStr) => {
  if (seleStr.includes('-')) {
    const defaultProps = ['no', '_id', 'bisId']
    defaultProps.forEach(key => {
      if (!seleStr.includes(key)) seleStr += ` ${key}`
    })
  }
  return exports.selectProp(body, seleStr)
}


// 单位数字返回双字符串, 5 -> '05', 58 -> '58'
exports.numFmt2B = (n) => ('' + n)[1] ? '' + n : '0' + n

// 易懂数字显示: '1.58K'
exports.numFmtByUnit = (num, digits = 2) => {
  const si = [
    { value: 1e18, symbol: 'E' },
    { value: 1e15, symbol: 'P' },
    { value: 1e12, symbol: 'T' },
    { value: 1073741824, symbol: 'G' },
    { value: 1048576, symbol: 'M' },
    { value: 1024, symbol: 'K' },
    { value: 1, symbol: '' }
  ]
  const rx = /\.0+$|(\.[0-9]*[1-9])0+$/
  let i
  for (i of si) if (num >= i.value) break

  return (num / i.value).toFixed(digits).replace(rx, '$1') + i.symbol
}

// 3位加 ','
exports.toThousandslsFilter = num => (+num || 0).toString()
  .replace(/^-?\d+/g, m => m.replace(/(?=(?!\b)(\d{3})+$)/g, ','))

/**
 * 解码
 * 返回 'utf8', 'ascii','hex' 格式字符串
 * @param {string} str base64格式
 */
exports.atob = (str, encoding = 'utf8') => Buffer.from(str, 'base64').toString(encoding)
/**
 * 编码 base64 
 * str 格式,'utf8', 'ascii','hex'
 * @param {string|Buffer} str 默认: utf8格式
 */
exports.btoa = (str, encoding = 'utf8') => (Buffer.isBuffer(str) ? str : Buffer.from(str, encoding))
  .toString('base64')

/**
 * 延时，不影响其它的http进程
 * @param {number} ms 延时ms
 */
exports.delay = (ms = 1000) => {
  return new Promise(res => setTimeout(res, ms))
}
/**
 * 延时，卡死cpu！, nodejs主线程要等待！！，delay不影响其它进程
 * @param {number} ms 
 */
const sleep = (ms = 0) => {
  for (var start = new Date; new Date - start <= ms;) { }
}

exports.isEmpty = (obj) => obj === undefined || obj === null
  || (typeof obj === 'object' && Object.keys(obj).length === 0)
  || (typeof obj === 'string' && obj.trim().length === 0)
  || (Array.isArray(obj) && obj.length === 0)

exports.isNull = (obj) => obj === null

exports.isUndefined = (obj) => typeof obj === 'undefined'

exports.isObject = (obj) => typeof obj === 'object'
  && obj !== null
  && !Array.isArray(obj)

exports.isString = (obj) => typeof obj === 'string'

exports.isFunction = (obj) => typeof obj === 'function'

// null, undefined, object, array, string, number, boolen, 
// function, regexp, map, set, symbol, blob, 
exports.type = (o) => Object.prototype.toString.call(o)
  .match(/\[object (.*?)\]/)[1]
  .toLowerCase()

// 显示当天 年月日
exports.dateYMD = () => {
  let date = new Date()
  const year = date.getFullYear()
  const month = date.getMonth() + 1
  const day = date.getDate()
  // const hour = date.getHours()
  // const minute = date.getMinutes()
  // const second = date.getSeconds()
  return [year, month, day].map(formatNumber).join('-') //+ ' ' + [hour, minute, second].map(formatNumber).join(':')
}

/*
 *拓展Date方法。得到格式化的日期形式
 *date.format('yyyy-MM-dd')，date.format('yyyy/MM/dd'),date.format('yyyy.MM.dd')
 *date.format('dd.MM.yy'), date.format('yyyy.dd.MM'), date.format('yyyy-MM-dd HH:mm')
 *使用方法 如下：
 *                       var date = new Date();
 *                       var todayFormat = date.format('yyyy-MM-dd'); //结果为2015-2-3
 *Parameters:
 *format - {string} 目标格式 类似('yyyy-MM-dd')
 *Returns - {string} 格式化后的日期 2015-2-3
 *
 */
Date.prototype.format = function (format) {
  var o = {
    'M+': this.getMonth() + 1, //month
    'd+': this.getDate(), //day
    'h+': this.getHours(), //hour
    'm+': this.getMinutes(), //minute
    's+': this.getSeconds(), //second
    'q+': Math.floor((this.getMonth() + 3) / 3), //quarter
    S: this.getMilliseconds() //millisecond
  }
  if (/(y+)/.test(format))
    format = format.replace(RegExp.$1, (this.getFullYear() + '').substr(4 - RegExp.$1.length))
  Object.keys(o).forEach(k => {
    if (new RegExp(`(${k})`).test(format))
      format = format.replace(
        RegExp.$1,
        RegExp.$1.length == 1 ? o[k] : ('00' + o[k]).substr(('' + o[k]).length)
      )
  })
  return format
}

exports.toHump = (str, sign = '_') => {
  const re = new RegExp(`\\${sign}(\\w)`, 'g')
  return str.replace(re, (match, letter) => letter.toUpperCase())
}

exports.toLine = (str, sign = '_') => {
  return str.replace(/([A-Z])/g, `${sign}$1`).toLowerCase()
}
// ms-> Xd X:XX:XX 
exports.timeFmt = (time) => {
  let s = Math.floor(time / 1000)
  let m = Math.floor(s / 60)
  let h = Math.floor(s / (60 * 60))
  let d = Math.floor(s / (24 * 60 * 60))
  m = m - h * 60
  h = h % 24
  s = s % 60

  let res = d > 0 ? `${d}d ` : ''
  res += h > 0 ? `${h}:` : ''
  res += m > 0 ? `${exports.numFmt2B(m)}:${exports.numFmt2B(s)}` : `${exports.numFmt2B(s)}s`

  return res
}
// 延时ms
exports.wait = ms => new Promise(resolve => setTimeout(resolve, ms))
// obj -> [key1,val1,key2,val2,...]
exports.objToArr = obj => Object.entries(obj).reduce((acc, arr) => acc.concat(arr), [])
