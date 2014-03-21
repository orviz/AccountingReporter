metric = {
    "aggregate": {
        "foo": {
            "group": ["group1", "group2"],
            "project": ["project1"],
        },
        "bar": {
            "group": ["foobar"],
        }
    },
    "metric": {
        "foo": {
            "collector": "GECollector",
            "aggregate": "foo",
            "metric": "cpu",
            "start_time": "",
            "end_time": "",
            "chart": "pie"
        }
    }
}


filter_map = (
    (
        [],
        {}
    ),
    (
        ["foo", "bar", "baz"],
        {'IN': {'baz', 'foo', 'bar'}}
    ),
    (
        ["foo", "bar", "foo", "baz", "bar", "baz"],
        {'IN': {'baz', 'foo', 'bar'}}
    ),
    (
        ["*"],
        {'IN': {'*'}}
    ),
    (
        ["!foo", "bar", "baz"],
        {'NOT IN': {'foo'}, 'IN': {'baz', 'bar'}}
    ),
    (
        ["foo*", "*bar", "b*z"],
        {'CONTAINS': {'foo*', '*bar', 'b*z'}}
    ),
    (
        ["!foo*", "!*bar", "!b*z"],
        {'NOT CONTAINS': {'foo*', '*bar', 'b*z'}}
    ),
    (
        ["!*"],
        {'NOT CONTAINS': {'*'}}
    ),
    (
        ["foo", "!bar", "*baz", "!bazonk*"],
        {'IN': {'foo'},
         'NOT IN': {'bar'},
         'CONTAINS': {'*baz'}, 'NOT CONTAINS': {'bazonk*'}}
    ),
)
