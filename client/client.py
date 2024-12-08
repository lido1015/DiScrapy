import sys
import argparse
from streamlit.web import cli



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ip", type=str, default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--entry_addr", type=str, default="127.0.0.1:10000")

    try:
        args = parser.parse_args()    
        cli.main_run(["client_node.py", "--server.address", args.ip, "--server.port", args.port, "--" , "--entry_addr", args.entry_addr])        
        
    except SystemExit as e:
        print(f"Error: {e}, argumentos recibidos: {sys.argv}")   


if __name__ == "__main__":
    main()




   


