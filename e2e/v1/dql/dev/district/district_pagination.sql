#package('github.com/viant/datly/e2e/v1/shape/dev/district')
#setting($_ = $connector('dev'))
#setting($_ = $route('/v1/api/shape/dev/meta/districts', 'GET'))

#set( $_ = $Page<int>(query/page).Optional().QuerySelector('districts'))
#set( $_ = $Data<?>(output/view).Embed())


SELECT districts.*,
       cities.*,
       set_limit(cities, 2)
FROM (SELECT t.* FROM DISTRICT t WHERE 1 = 1 AND ID IN ($IDs)) districts
JOIN (SELECT * FROM CITY t) cities ON districts.ID = cities.DISTRICT_ID
