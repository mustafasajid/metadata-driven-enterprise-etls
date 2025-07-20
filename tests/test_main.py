import sys
from io import StringIO
from src.main import main

def test_main_prints_welcome_message():
    captured_output = StringIO()
    sys.stdout = captured_output
    main()
    sys.stdout = sys.__stdout__
    assert "Welcome to the BYOD Enterprise solution Databricks project!" in captured_output.getvalue()