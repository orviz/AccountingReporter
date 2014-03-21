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
