import { test } from '@japa/runner'
import { parse } from '../src/utils.js'
import { E_INVALID_DURATION_EXPRESSION } from '../src/exceptions.js'

test.group('Utils', () => {
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
