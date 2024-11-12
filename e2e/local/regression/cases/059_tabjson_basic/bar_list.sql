/* {"URI":"basic/bars/", "DataFormat":"tabular", "TabularJSON":{"FloatPrecision":"8"}} */

#set( $_ = $Data<?>(output/view).Embed())

SELECT bar.*
FROM (SELECT * FROM BAR t ) bar
