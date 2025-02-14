RECURSE(
    apps
    docs
    core
    library
    meta
    mvp
    public
    services
    tools
    yql_docs
)

IF(NOT EXPORT_CMAKE)
  RECURSE(
    tests
  )
ENDIF()
