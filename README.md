# MASDU data sender project

The MASDU Data Sender project is responsible for retrieving archive data from the FAST/TOOLS SCADA system and creating specialized reports. 
These reports are then sent to a specified FTP server.


## Features
***
- Read data from the FAST/TOOLS SCADA database
- Store reports locally in a specific XML format
- Send reports to the FTP server at specified times (depending on the report type)

## Installation
***
1. Install python 3.7
2. Clone project from Github
   ```
   git clone git@github.com:ead3471/masdu.git
   ```
3. Create a virtual environment with Python 3.7:
   ```
   virtualenv -p `which python3.7` venv
   ```

4. Install requirements:
    ```
    pip install -r requirements.txt
    ```  

4. Add the location of dss.pyd (distributed with FAST/TOOLS) to the PYTHONPATH.

5. At this moment script tuned to work with distinct FAST/TOOLS project. 
You can change reports by rewrite xml files in setup folder.

6. In command line navigate to srs/asdu/data_load/ folder

7. Run script
    ```
    python m_asdu_handler.py
    ```

    
        



