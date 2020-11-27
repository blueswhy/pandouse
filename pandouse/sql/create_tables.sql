-- stk_bar_1min
DROP TABLE IF EXISTS ark.b_stk_bar_1min;
CREATE TABLE ark.b_stk_bar_1min
(
  `symbol` String,
  `t_date` DateTime,
  `s_time` DateTime,
  `e_time` DateTime,

  `open`   Float32,
  `high`   Float32,
  `low`    Float32,
  `close`  Float32,
  `volume` Float32,
  `amount` Float32,
  `change` Float32,
  `ret`    Float32
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(t_date)
ORDER BY (symbol, s_time)
SAMPLE BY symbol
SETTINGS index_granularity = 8192;

-- stk_bar_5min
DROP TABLE IF EXISTS ark.b_stk_bar_5min;
CREATE TABLE ark.b_stk_bar_5min
(
  `symbol` String,
  `t_date` DateTime,
  `s_time` DateTime,
  `e_time` DateTime,

  `open`   Float32,
  `high`   Float32,
  `low`    Float32,
  `close`  Float32,
  `volume` Float32,
  `amount` Float32,
  `change` Float32,
  `ret`    Float32
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(t_date)
ORDER BY (symbol, s_time)
SAMPLE BY symbol
SETTINGS index_granularity = 8192;

-- b_stk_trade
DROP TABLE IF EXISTS ark.b_stk_trade;
CREATE TABLE ark.b_stk_trade
(
	`symbol`        String,
	`trading_date`  DateTime64(6, 'Asia/Shanghai'),
	`trading_time`  DateTime64(6, 'Asia/Shanghai'),
	`rec_id`        Int32,
	`trade_channel` Int16,
	`trade_price`   Float32,
	`trade_volume`  Int32,
	`trade_amount`  Float32,
	`unix`          Int64,
	`market`        String,
	`buy_rec_id`    Int32,
	`sell_rec_id`   Int32,
	`buy_sell_flag` String,
	`security_id`   Int64,
	`symbol_source` Nullable(String)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(trading_date)
ORDER BY (trading_date, symbol)
SAMPLE BY symbol
SETTINGS index_granularity = 8192;