# 개요
- [Bitfinex](https://www.bitfinex.com/) 특정통화의 캔들(candle)을 DB(MySQL)에 저장
  - https://docs.bitfinex.com/v2/reference#rest-public-candles
  - 시가, 종가, 저가, 고가, 거래량 (open, close, low, high, volume)
- DB가 비어있지 않으면, 마지막 캔들부터 이어서 저장
- 스크립트를 실행 시켜두면, 계속 캔들을 추가 저장

# 환경
- Python 3.6 (64bit)
- MySQL 5.6.37

# 실행 인자
### `통화타입 캔들타입 디비테이블명 [로그레벨]`
- 비트코인의 달러 1분 캔들을 `candle_btcusd_1m` 테이블에 저장
  - `python save_bitfinex_candles.py tBTCUSD 1m candle_btcusd_1m`
- 통화타입
  - [Bitfinex 통화 표기](https://docs.bitfinex.com/v2/docs/readme#section-what-is-a-symbol-)
    - `tBTCUSD, tETHUSD ...`
- 캔들타입
  - `1m, 5m, 15m, 30m, 1h, 3h, 6h, 12h, 1D, 7D, 14D, 1M`
- 디비테이블명
  - 저장할 테이블 이름
- [로그레벨]
  - 필수 아님 (기본값 INFO)
  - DEBUG, INFO, WARNING, ERROR, CRITICAL

# 저장 테이블 스키마
```
CREATE TABLE `candle_btcusd_1h` (
  `id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
  `start_timestamp` TIMESTAMP NOT NULL,
  `open` DOUBLE NOT NULL,
  `high` DOUBLE NOT NULL,
  `low` DOUBLE NOT NULL,
  `close` DOUBLE NOT NULL,
  `volume` DOUBLE NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `unq_start_timestamp` (`start_timestamp`)
) ENGINE=INNODB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
```
