#package('github.com/viant/datly/e2e/v1/shape/dev/user')
#setting($_ = $connector('dev'))
#setting($_ = $route('/v1/api/shape/dev/users/', 'GET'))

SELECT user.* EXCEPT MGR_ID,
       self_ref(user, 'Team', 'ID', 'MGR_ID')
FROM (SELECT t.* FROM USER t  ) user
