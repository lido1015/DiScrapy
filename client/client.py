import sys
import argparse
from streamlit.web import cli

SL_HOST = "0.0.0.0"
SL_PORT = 8501

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", type=str, default="10.0.11.2")

    args = parser.parse_args()  
    cli.main_run(["client_node.py", "--server.address", SL_HOST, "--server.port", SL_PORT , "--" , "--server", args.server])        




   


