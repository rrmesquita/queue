import { test } from '@japa/runner'
import { parse, resolveRetention } from '../src/utils.js'
import { E_INVALID_DURATION_EXPRESSION } from '../src/exceptions.js'

test.group('Utils | parse', () => {
  test('parse should return number when input is number', ({ assert, expectTypeOf }) => {
    const result = parse(5000)

    assert.equal(result, 5000)
    expectTypeOf(result).toBeNumber()
  })

  test('parse should parse duration strings', ({ assert }) => {
    assert.equal(parse('1s'), 1000)
    assert.equal(parse('2m'), 120000)
    assert.equal(parse('1h'), 3600000)
    assert.equal(parse('500ms'), 500)
  })

  test('parse should throw error for invalid duration strings', ({ assert }) => {
    assert.plan(1)

    try {
      parse('invalid')
    } catch (error) {
      assert.instanceOf(error, E_INVALID_DURATION_EXPRESSION)
    }
  })
})

test.group('Utils | resolveRetention', () => {
  test('undefined retention should return keep: false', ({ assert }) => {
    const result = resolveRetention(undefined)

    assert.deepEqual(result, { keep: false, maxAge: 0, maxCount: 0 })
  })

  test('true retention should return keep: false (remove on complete)', ({ assert }) => {
    const result = resolveRetention(true)

    assert.deepEqual(result, { keep: false, maxAge: 0, maxCount: 0 })
  })

  test('false retention should return keep: true (keep in history)', ({ assert }) => {
    const result = resolveRetention(false)

    assert.deepEqual(result, { keep: true, maxAge: 0, maxCount: 0 })
  })

  test('object with count should return keep: true with maxCount', ({ assert }) => {
    const result = resolveRetention({ count: 100 })

    assert.deepEqual(result, { keep: true, maxAge: 0, maxCount: 100 })
  })

  test('object with age as number should return keep: true with maxAge', ({ assert }) => {
    const result = resolveRetention({ age: 3600000 })

    assert.deepEqual(result, { keep: true, maxAge: 3600000, maxCount: 0 })
  })

  test('object with age as string should parse and return maxAge', ({ assert }) => {
    const result = resolveRetention({ age: '1h' })

    assert.deepEqual(result, { keep: true, maxAge: 3600000, maxCount: 0 })
  })

  test('object with both age and count should return both', ({ assert }) => {
    const result = resolveRetention({ age: '30m', count: 50 })

    assert.deepEqual(result, { keep: true, maxAge: 1800000, maxCount: 50 })
  })
})
