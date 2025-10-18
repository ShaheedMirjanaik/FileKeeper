import socket
import base64
import json
import os
import sys
from typing import List, Optional, Tuple

BUFFER_SIZE = 1024 * 1024  # 1MB buffer for large files
MAX_COMMAND_LENGTH = BUFFER_SIZE * 10  # 10MB max command size

class FileClient:
    def __init__(self, nodes: List[Tuple[str, int]]):
        self.nodes = nodes
        self.current_node = 0
        self.session: Optional[socket.socket] = None
        self.authenticated = False

    def _connect_to_next_node(self) -> bool:
        if self.session:
            try:
                self.session.close()
            except:
                pass
            self.session = None
            self.authenticated = False

        attempts = 0
        while attempts < len(self.nodes):
            try:
                host, port = self.nodes[self.current_node]
                print(f"Connecting to {host}:{port}...")
                self.session = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                # Set larger buffer sizes
                self.session.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, BUFFER_SIZE)
                self.session.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, BUFFER_SIZE)
                self.session.settimeout(30)  # 30 second timeout
                self.session.connect((host, port))
                return True
            except Exception as e:
                print(f"Failed to connect to {host}:{port}: {e}")
                self.current_node = (self.current_node + 1) % len(self.nodes)
                attempts += 1

        return False

    def _ensure_connected(self) -> bool:
        if not self.session:
            return self._connect_to_next_node()
        return True

    def _send_command(self, command: str) -> Optional[str]:
        """Helper method to send commands and receive responses with retries"""
        if not self._ensure_connected():
            print("Not connected to server")
            return None

        try:
            print(f"Sending command: {command[:20]}...")  # Show first 20 chars of command
            # Send command in chunks if needed
            total_sent = 0
            command_bytes = command.encode()
            while total_sent < len(command_bytes):
                sent = self.session.send(command_bytes[total_sent:total_sent + BUFFER_SIZE])
                if sent == 0:
                    raise RuntimeError("Socket connection broken")
                total_sent += sent
            print(f"Sent {total_sent} bytes")

            # Now receive the response
            self.session.settimeout(30)  # 30 seconds timeout
            try:
                response = self.session.recv(BUFFER_SIZE).decode()
                print(f"Received response: {response}")
                return response
            except socket.timeout:
                print("Timeout waiting for server response")
                return None
            except Exception as e:
                print(f"Error receiving response: {e}")
                return None

        except Exception as e:
            print(f"Error in send_command: {e}")
            self.session = None  # Force reconnect on next try
            return None

        except Exception as e:
            print(f"Communication error: {e}")
            self.session = None  # Force reconnect on next try
            return None

    def authenticate(self, username: str, password: str) -> bool:
        try:
            print(f"\nSending authentication request for user: {username}")
            command = f"AUTH {username} {password}"
            print("Waiting for server response...")
            response = self._send_command(command)
            print(f"Server response: {response}")
            
            if response and response.startswith("OK"):
                print("Authentication successful!")
                self.authenticated = True
                return True
            print(f"Authentication failed: {response if response else 'No response from server'}")
            return False
        except Exception as e:
            print(f"Authentication error: {e}")
            return False

    def upload_file(self, filepath: str) -> bool:
        if not self.authenticated:
            print("Error: Not authenticated. Please login first.")
            return False
            
        if not os.path.exists(filepath):
            print(f"Error: File '{filepath}' not found.")
            return False

        try:
            print(f"Reading file: {filepath}")
            file_size = os.path.getsize(filepath)
            print(f"File size: {file_size} bytes")

            if file_size > MAX_COMMAND_LENGTH:
                print(f"Error: File is too large. Maximum size is {MAX_COMMAND_LENGTH // (1024*1024)}MB")
                return False

            with open(filepath, "rb") as f:
                file_content = f.read()
            
            filename = os.path.basename(filepath)
            print(f"Preparing to upload: {filename}")
            
            # Encode file content
            encoded_content = base64.b64encode(file_content).decode('utf-8')
            command = f"UPLOAD {filename} {encoded_content}"
            
            response = self._send_command(command)
            if response and response.startswith("OK"):
                print("Upload successful!")
                return True
            else:
                print(f"Upload failed: {response if response else 'No response from server'}")
                return False
                
        except Exception as e:
            print(f"Upload error: {str(e)}")
            return False

    def download_file(self, filename: str, save_path: str) -> bool:
        if not self.authenticated:
            print("Error: Not authenticated. Please login first.")
            return False

        try:
            print(f"Requesting file: {filename}")
            response = self._send_command(f"DOWNLOAD {filename}")
            
            if response and response.startswith("OK"):
                encoded_content = response[3:]  # Skip "OK "
                print("Decoding file content...")
                try:
                    file_content = base64.b64decode(encoded_content)
                except Exception as e:
                    print(f"Error decoding file content: {e}")
                    return False

                print(f"Saving file to: {save_path}")
                save_dir = os.path.dirname(os.path.abspath(save_path))
                os.makedirs(save_dir, exist_ok=True)
                
                with open(save_path, "wb") as f:
                    f.write(file_content)
                print("File saved successfully!")
                return True
            else:
                print(f"Download failed: {response if response else 'No response from server'}")
                return False
        except Exception as e:
            print(f"Download error: {str(e)}")
            return False

    def delete_file(self, filename: str) -> bool:
        if not self.authenticated:
            print("Error: Not authenticated. Please login first.")
            return False

        try:
            print(f"Requesting to delete: {filename}")
            response = self._send_command(f"DELETE {filename}")
            
            if response and response.startswith("OK"):
                print("Delete successful!")
                return True
            else:
                print(f"Delete failed: {response if response else 'No response from server'}")
                return False
        except Exception as e:
            print(f"Delete error: {str(e)}")
            return False

    def list_files(self) -> List[str]:
        if not self.authenticated:
            print("Error: Not authenticated. Please login first.")
            return []

        try:
            print("Requesting file list...")
            response = self._send_command("LIST")
            
            if response and response.startswith("OK"):
                files_json = response[3:]  # Skip "OK "
                try:
                    return json.loads(files_json)
                except json.JSONDecodeError as e:
                    print(f"Error parsing file list: {e}")
                    return []
            else:
                print(f"List failed: {response if response else 'No response from server'}")
                return []
        except Exception as e:
            print(f"List error: {str(e)}")
            return []

    def close(self):
        if self.session:
            try:
                self.session.close()
            except:
                pass
        self.session = None
        self.authenticated = False

def main():
    # Default to localhost:8080 for simplicity
    nodes = [("localhost", 8080)]
    client = None
    
    try:
        print("\nConnecting to server...")
        client = FileClient(nodes)
        
        if not client._ensure_connected():
            print("✗ Failed to connect to server! Make sure the server is running.")
            return

        print("✓ Connected to server successfully!")
        
        while True:
            print("\n=== Distributed File System Client ===")
            if not client.authenticated:
                print("1. Login")
                print("2. Exit")
            else:
                print("1. Upload file")
                print("2. Download file")
                print("3. Delete file")
                print("4. List files")
                print("5. Logout")
                print("6. Exit")
            
            try:
                choice = input("\nEnter your choice: ").strip()
            except KeyboardInterrupt:
                print("\nExiting...")
                break

            if not client.authenticated:
                if choice == "1":
                    username = input("Enter username: ")
                    password = input("Enter password: ")
                    if client.authenticate(username, password):
                        print("\n✓ Login successful!")
                    else:
                        print("\n✗ Login failed. Invalid credentials.")
                elif choice == "2":
                    print("\nGoodbye!")
                    break
                else:
                    print("\n✗ Invalid choice!")
                continue

            # Handle authenticated user choices
            if choice == "1":
                filepath = input("\nEnter the path of the file to upload: ").strip()
                if not filepath:
                    print("\n✗ No file path provided!")
                    continue
                    
                filepath = os.path.abspath(filepath)
                if not os.path.exists(filepath):
                    print(f"\n✗ Error: File not found at '{filepath}'!")
                    continue
                
                if client.upload_file(filepath):
                    print("\n✓ File uploaded successfully!")
                else:
                    print("\n✗ Upload failed!")
                    
            elif choice == "2":
                filename = input("\nEnter the filename to download: ").strip()
                if not filename:
                    print("\n✗ No filename provided!")
                    continue
                    
                save_path = input("Enter where to save the file: ").strip()
                if not save_path:
                    print("\n✗ No save path provided!")
                    continue
                
                if client.download_file(filename, save_path):
                    print("\n✓ File downloaded successfully!")
                else:
                    print("\n✗ Download failed!")
                    
            elif choice == "3":
                filename = input("\nEnter the filename to delete: ").strip()
                if not filename:
                    print("\n✗ No filename provided!")
                    continue
                
                confirm = input(f"Are you sure you want to delete '{filename}'? (y/N): ").lower()
                if confirm != 'y':
                    print("\nDelete operation cancelled.")
                    continue
                
                if client.delete_file(filename):
                    print("\n✓ File deleted successfully!")
                else:
                    print("\n✗ Delete failed!")
                    
            elif choice == "4":
                files = client.list_files()
                if files:
                    print("\nAvailable files:")
                    for file in files:
                        print(f"- {file}")
                else:
                    print("\nNo files found or error occurred")
                    
            elif choice == "5":
                client.authenticated = False
                print("\n✓ Logged out successfully!")
                
            elif choice == "6":
                print("\nGoodbye!")
                break
                
            else:
                print("\n✗ Invalid choice!")

            input("\nPress Enter to continue...")

    except KeyboardInterrupt:
        print("\n\nExiting...")
    except Exception as e:
        print(f"\n✗ An error occurred: {str(e)}")
    finally:
        if client:
            client.close()

if __name__ == "__main__":
    main()