case {os:getenv("CIRCLECI"), os:getenv("COVERALLS_REPO_TOKEN")} of
    {"true", Token} when is_list(Token) ->
        JobId   = os:getenv("CIRCLE_BUILD_NUM"),
        CONFIG1 = lists:keystore(coveralls_service_job_id, 1, CONFIG, {coveralls_service_job_id, JobId}),
        lists:keystore(coveralls_repo_token, 1, CONFIG1, {coveralls_repo_token, Token});
    _ ->
        CONFIG
end.
