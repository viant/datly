/* {"URI":"comprehensive/bars/", "DataFormat":"tabular", "TabularJSON":{"FloatPrecision":"-1"}} */


#set( $_ = $Data<?>(output/view).Cardinality('One').WithTag('anonymous:"true"'))


SELECT bar.*
FROM (SELECT * FROM BAR t ) bar
