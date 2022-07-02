from prefect.agent.local import LocalAgent

LocalAgent(
    import_paths=['.flows'],
    show_flow_logs=True,
    ).start()