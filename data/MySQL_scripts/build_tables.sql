
CREATE TABLE `candles` (
   `id` int(11) NOT NULL AUTO_INCREMENT,
   `symbol` varchar(20),
   `open_date` datetime,
   `open` float(20,9),
   `high` float(20,9),
   `low` float(20,9),
   `close` float(20,9),
   `volume` float(20,9),
   `close_date` datetime,
   `quote_asset_volume` float(20,9),
   `number_of_trades` int(11),
   `taker_buy_base_asset_volume` float(20,9),
   `taker_buy_quote_asset_volume` float(20,9),
   PRIMARY KEY (`id`),
   UNIQUE KEY `stamp` (`open_date`,`symbol`)
);

create table `ticker` (
  `date` datetime,
  `symbol` varchar(20),
  `price` float(20,9),
  UNIQUE KEY `stamp` (`date`,`symbol`)
);

create table `user_symbols` (
  `symbol` varchar(20),
  `from_symbol` varchar(20),
  `to_symbol` varchar(20),
  `exchange` varchar(40),
  UNIQUE KEY `stamp` (`symbol`, `exchange`)
);

create table `all_symbols` (
  `symbol` varchar(20),
  `from_symbol` varchar(20),
  `to_symbol` varchar(20),
  `exchange` varchar(40),
  UNIQUE KEY `stamp` (`symbol`, `exchange`)
);

create table `buys` (
  `id` varchar(10),
  `symbol` varchar(20),
  `date` datetime,
  `price` float,
  `amount_fs` float,
  `amount_ts` float
);

create table `sells` (
  `id` varchar(10),
  `symbol` varchar(20),
  `date` datetime,
  `price` float,
  `amount_fs` float,
  `amount_ts` float,
  `profit` float,
  `percent_profit` float
);

create table `pending` (
  `id` varchar(10),
  `symbol` varchar(20),
  `date` datetime,
  `price` float,
  `amount_ts` float,
  `amount_fs` float
);

create table `test_buys` (
  `id` varchar(10),
  `symbol` varchar(20),
  `date` datetime,
  `price` float,
  `amount_fs` float,
  `amount_ts` float
);

create table `test_sells` (
  `id` varchar(10),
  `symbol` varchar(20),
  `date` datetime,
  `price` float,
  `amount_fs` float,
  `amount_ts` float,
  `profit` float,
  `percent_profit` float
);

create table `test_pending` (
  `id` varchar(10),
  `symbol` varchar(20),
  `date` datetime,
  `price` float,
  `amount_ts` float,
  `amount_fs` float
);
