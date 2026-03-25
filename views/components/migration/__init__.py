"""
Migration Engine components — Phase 2 split targets.

    step_config.py       render_step_config()        — Step 1: select/upload config
    step_connections.py  render_step_connections()   — Step 2: test source & target DB
    step_review.py       render_step_review()        — Step 3: review settings, checkpoint
    step_execution.py    render_step_execution()     — Step 4: run ETL, progress, rollback
"""
