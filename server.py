import socket
import threading
import json
import os
import base64
from typing import Dict, Optional

BUFFER_SIZE = 1024 * 1024  # 1MB buffer for large files

class FileServer:
    def __init__(self, host: str, port: int, storage_dir: str = "storage"):
        self.host = host
        self.port = port
        self.storage_dir = storage_dir
        self.users = self._load_users()
        self._ensure_storage_exists()
        
    def _load_users(self) -> Dict[str, str]:
        try:
            with open("users.json", "r") as f:
                return json.load(f)
        except FileNotFoundError:
            print("Warning: users.json not found. Creating empty users file.")
            with open("users.json", "w") as f:
                json.dump({}, f)
            return {}

    def _ensure_storage_exists(self):
        if not os.path.exists(self.storage_dir):
            os.makedirs(self.storage_dir)

    def _get_user_dir(self, username: str) -> str:
        user_dir = os.path.join(self.storage_dir, username)
        if not os.path.exists(user_dir):
            os.makedirs(user_dir)
        return user_dir

    def _recv_all(self, client_socket: socket.socket, timeout: int = 30) -> bytes:
        """Receive all data from the socket"""
        client_socket.settimeout(timeout)
        chunks = []
        while True:
            try:
                chunk = client_socket.recv(BUFFER_SIZE)
                if not chunk:
                    break
                chunks.append(chunk)
                if len(chunk) < BUFFER_SIZE:
                    break
            except socket.timeout:
                break
        return b''.join(chunks)

    def handle_client(self, client_socket: socket.socket, address: str):
        print(f"New connection from {address}")
        authenticated_user: Optional[str] = None

        # Set socket buffer sizes
        client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, BUFFER_SIZE)
        client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, BUFFER_SIZE)

        try:
            while True:
                try:
                    # Receive data with timeout
                    # Receive command with standard recv first
                    data = client_socket.recv(BUFFER_SIZE)
                    if not data:
                        print(f"Client {address} disconnected - no data received")
                        break
                    
                    command = data.decode().strip()
                    print(f"Raw command received from {address}: {command}")
                    
                    if not command:
                        print(f"Client {address} sent empty command")
                        continue

                    parts = command.split(maxsplit=2)  # Split into max 3 parts: command, arg1, remaining
                    if not parts:
                        print(f"Client {address} sent invalid command format")
                        client_socket.send("ERROR Invalid command format".encode())
                        continue
                        
                    cmd = parts[0].upper()
                    print(f"Processing command: {cmd} from {address}")

                    if cmd == "AUTH":
                        if len(parts) != 3:
                            print(f"Invalid auth command format from {address} - Expected 3 parts, got {len(parts)}")
                            client_socket.send("ERROR Invalid auth command".encode())
                            continue

                        username, password = parts[1], parts[2]
                        print(f"Auth attempt from {address}")
                        print(f"Username: {username}")
                        print(f"Current users in system: {list(self.users.keys())}")
                        
                        if username in self.users:
                            stored_password = self.users[username]
                            if stored_password == password:
                                authenticated_user = username
                                response = "OK Authenticated"
                                print(f"User {username} authenticated successfully from {address}")
                            else:
                                response = "ERROR Invalid password"
                                print(f"Invalid password for user {username} from {address}")
                                print(f"Received password length: {len(password)}")
                                print(f"Stored password length: {len(stored_password)}")
                        else:
                            response = "ERROR User not found"
                            print(f"User {username} not found in users list")
                        
                        print(f"Sending auth response: {response}")
                        client_socket.send(response.encode())

                    elif authenticated_user is None:
                        client_socket.send("ERROR Authentication required".encode())
                        print(f"Unauthenticated command attempt from {address}")

                    elif cmd == "UPLOAD":
                        if len(parts) < 3:
                            client_socket.send("ERROR Invalid upload command".encode())
                            continue

                        try:
                            filename = parts[1]
                            file_data = parts[2]
                            
                            print(f"Processing upload for {filename} from {authenticated_user}")
                            file_content = base64.b64decode(file_data)
                            file_path = os.path.join(self._get_user_dir(authenticated_user), filename)
                            
                            with open(file_path, "wb") as f:
                                f.write(file_content)
                            client_socket.send("OK File uploaded".encode())
                            print(f"File {filename} uploaded successfully")
                        except Exception as e:
                            error_msg = f"ERROR Upload failed: {str(e)}"
                            print(f"Upload error: {error_msg}")
                            client_socket.send(error_msg.encode())

                    elif cmd == "DOWNLOAD":
                        if len(parts) != 2:
                            client_socket.send("ERROR Invalid download command".encode())
                            continue

                        filename = parts[1]
                        file_path = os.path.join(self._get_user_dir(authenticated_user), filename)
                        
                        try:
                            print(f"Processing download for {filename} by {authenticated_user}")
                            with open(file_path, "rb") as f:
                                file_content = f.read()
                            encoded_content = base64.b64encode(file_content).decode()
                            response = f"OK {encoded_content}"
                            client_socket.send(response.encode())
                            print(f"File {filename} downloaded successfully")
                        except FileNotFoundError:
                            client_socket.send("ERROR File not found".encode())
                            print(f"File {filename} not found")
                        except Exception as e:
                            error_msg = f"ERROR Download failed: {str(e)}"
                            print(f"Download error: {error_msg}")
                            client_socket.send(error_msg.encode())

                    elif cmd == "DELETE":
                        if len(parts) != 2:
                            client_socket.send("ERROR Invalid delete command".encode())
                            continue

                        filename = parts[1]
                        file_path = os.path.join(self._get_user_dir(authenticated_user), filename)
                        
                        try:
                            print(f"Processing delete for {filename} by {authenticated_user}")
                            os.remove(file_path)
                            client_socket.send("OK File deleted".encode())
                            print(f"File {filename} deleted successfully")
                        except FileNotFoundError:
                            client_socket.send("ERROR File not found".encode())
                            print(f"File {filename} not found")
                        except Exception as e:
                            error_msg = f"ERROR Delete failed: {str(e)}"
                            print(f"Delete error: {error_msg}")
                            client_socket.send(error_msg.encode())

                    elif cmd == "LIST":
                        try:
                            print(f"Processing list request for {authenticated_user}")
                            user_dir = self._get_user_dir(authenticated_user)
                            files = os.listdir(user_dir)
                            response = "OK " + json.dumps(files)
                            client_socket.send(response.encode())
                            print(f"File list sent to {authenticated_user}")
                        except Exception as e:
                            error_msg = f"ERROR List failed: {str(e)}"
                            print(f"List error: {error_msg}")
                            client_socket.send(error_msg.encode())

                    else:
                        client_socket.send("ERROR Unknown command".encode())
                        print(f"Unknown command received: {cmd}")

                except socket.timeout:
                    continue  # Just try to receive again
                    
        except Exception as e:
            print(f"Error handling client {address}: {e}")
        finally:
            try:
                client_socket.close()
            except:
                pass
            print(f"Connection closed from {address}")

    def start(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # Set server socket buffer sizes
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, BUFFER_SIZE)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, BUFFER_SIZE)
        
        server_socket.bind((self.host, self.port))
        server_socket.listen(5)
        
        print(f"Server started on {self.host}:{self.port}")
        
        try:
            while True:
                client_socket, address = server_socket.accept()
                client_thread = threading.Thread(
                    target=self.handle_client,
                    args=(client_socket, str(address))
                )
                client_thread.daemon = True
                client_thread.start()
        except KeyboardInterrupt:
            print("\nShutting down server...")
        finally:
            try:
                server_socket.close()
            except:
                pass

if __name__ == "__main__":
    server = FileServer("localhost", 8080)
    server.start()