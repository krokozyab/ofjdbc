import os
import subprocess
import jpype
import jaydebeapi
from dotenv import load_dotenv


# List installed Java versions
# /usr/libexec/java_home -V
java_version = '20'

# Set JAVA_HOME
try:
    java_home = subprocess.check_output(['/usr/libexec/java_home', '-v', java_version]).decode('utf-8').strip()
    os.environ['JAVA_HOME'] = java_home
    print(f"Successfully set JAVA_HOME to: {java_home}")
except subprocess.CalledProcessError as e:
    print(f"Error finding Java home: {e}")

# Verify JAVA_HOME is set
java_home = os.getenv('JAVA_HOME')
if not java_home:
    raise ValueError("JAVA_HOME environment variable is not set")

# Get the absolute path to your JDBC driver
jdbc_driver_path = os.path.abspath("/path_to_driver_file/orfujdbc-1.0-SNAPSHOT.jar")

# Verify the JAR file exists
if not os.path.exists(jdbc_driver_path):
    raise FileNotFoundError(f"JDBC driver JAR not found at: {jdbc_driver_path}")

# Start JVM with the correct classpath
if not jpype.isJVMStarted():
    jpype.startJVM(
        jpype.getDefaultJVMPath(),
        f"-Djava.class.path={jdbc_driver_path}",
        "-ea"  # enable assertions
    )

# JDBC connection parameters
driver_class = "my.jdbc.wsdl_driver.WsdlDriver"
jdbc_url = "jdbc:wsdl://<you-server>.oraclecloud.com/xmlpserver/services/ExternalReportWSSService?WSDL:/Custom/Financials/RP_ARB.xdo"

# Load environment variables from .env file
load_dotenv()

username = os.getenv('DB_USERNAME')
password = os.getenv('DB_PASSWORD')


try:
    # Connect to the database
    connection = jaydebeapi.connect(
        driver_class,
        jdbc_url,
        [username, password],
        jdbc_driver_path  # Pass the path directly, not as a list
    )
    print("Successfully connected to the database")

    # Create a cursor object
    cursor = connection.cursor()
    print("Cursor created successfully")

    # Execute a query
    cursor.execute("SELECT * FROM gl_je_lines OFFSET 0 ROWS FETCH NEXT 50 ROWS ONLY")
    print("Query executed successfully")

    # Fetch the results
    results = cursor.fetchone()
    for row in results:
        print(row)

except Exception as e:
    print(f"Error occurred: {e}")
finally:
    # Clean up resources
    try:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()
        print("Connection closed successfully")
    except Exception as e:
        print(f"Error while closing connection: {e}")

# Shutdown JVM when done
try:
    if jpype.isJVMStarted():
        jpype.shutdownJVM()
        print("JVM shutdown successfully")
except Exception as e:
    print(f"Error shutting down JVM: {e}")
