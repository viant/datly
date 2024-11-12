/* {"URI":"xml/basic/bars/", "DataFormat":"xml", "XML":{"FloatPrecision":"8"}} */


#set( $_ = $Data<?>(output/view).Embed())


SELECT bar.*
FROM (SELECT * FROM BAR t ) bar
