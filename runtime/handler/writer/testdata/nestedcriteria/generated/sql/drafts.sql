SELECT * FROM (SELECT r.*, '' AS should_delete FROM test_drafts r
) drafts