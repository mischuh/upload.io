from schema import And, Optional, Or, Schema, Use

schema = Schema({
        'namespace': Use(str),
        'version': Use(str),
        'sources': {
            'testcatalog': {
                'source': {
                    'type': Use(str),
                    'uri': Use(str),
                    Optional('options', default={}): dict
                },
                'parser': {
                    'name': Use(str),
                    Optional('options', default={}): dict
                },
                'target': {
                    Optional('connection', default={}): {
                        'uri': Use(str),
                        Optional('table'): Use(str),
                        Optional('schema'): Use(str),
                        Optional('options', default={}): dict

                    },
                    Optional('options', default={}): dict
                },
                'fields': [
                    {
                        'name': Use(str),
                        'data_type': Use(str),
                        Optional('alias'):  str,
                        Optional('default'):  str,
                        Optional('transformations', default=[]): [
                            {
                                'type': And(
                                        Use(str),
                                        lambda x: x in ['rule', 'filter']
                                    ),
                                'task': {
                                    'name': And(
                                        Use(str),
                                        lambda x: x in [
                                            'replace',
                                            'regexreplace',
                                            'uppercase',
                                            'lambda',
                                            'date_format'
                                        ]),
                                    'operator': Or(
                                            {Use(str): object},
                                            Use(str),
                                            None
                                        )
                                },
                                Optional('order'): int
                            }
                        ]
                    }
                ]
            }
        }
    }
)
