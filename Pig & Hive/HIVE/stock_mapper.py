import sys
import datetime
 
for line in sys.stdin:
  line = line.strip()
  exchange,stock_symbol,date1,stock_price_open,stock_price_high,stock_price_low,stock_price_close,stock_volume,stock_price_adj_close = line.split('\t')
  print '\t'.join(['PRICE_TYPE_OPEN',stock_price_open ])
  print '\t'.join(['PRICE_TYPE_HIGH',stock_price_high ])
  print '\t'.join(['PRICE_TYPE_LOW',stock_price_low ])

