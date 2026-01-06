"""
Camera Server (Producer) - Module 1
------------------------------------
Mo phong thiet bi camera stream du lieu video qua TCP.
- Thu hinh tu webcam hoac file video
- Ma hoa frame sang Base64 va dong goi vao JSON
- Gui qua TCP socket den Spark consumer
"""

import socket
import json
import base64
import time
import cv2
import numpy as np


class CameraServerConfig:
    """Cau hinh cho Camera Server"""
    HOST = "localhost"
    PORT = 9999
    FRAME_WIDTH = 640
    FRAME_HEIGHT = 480
    FPS_LIMIT = 10  # Gioi han so frame moi giay de tranh qua tai mang


class CameraServer:
    def __init__(self, video_source=0):
        """
        Khoi tao Camera Server
        
        Tham so:
            video_source: 0 cho webcam, hoac duong dan den file video
        """
        self.video_source = video_source
        self.connection = None
        self.server_socket = None
        self.running = False
        
    def start_server(self):
        """Khoi dong TCP server va cho ket noi"""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        try:
            self.server_socket.bind((CameraServerConfig.HOST, CameraServerConfig.PORT))
            self.server_socket.listen(1)
            print(f"[Camera Server] Da khoi dong tai {CameraServerConfig.HOST}:{CameraServerConfig.PORT}")
            print("[Camera Server] Dang cho Spark consumer ket noi...")
            
            self.connection, address = self.server_socket.accept()
            print(f"[Camera Server] Da ket noi voi consumer: {address}")
            return True
            
        except Exception as e:
            print(f"[Camera Server] Loi khi khoi dong server: {e}")
            return False
    
    def encode_frame_to_base64(self, frame):
        """
        Ma hoa OpenCV frame sang chuoi Base64
        
        Tham so:
            frame: numpy array (anh OpenCV)
        Tra ve:
            Chuoi da ma hoa Base64
        """
        # Ma hoa frame sang dinh dang JPEG
        _, buffer = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 80])
        # Chuyen doi sang Base64
        base64_string = base64.b64encode(buffer).decode('utf-8')
        return base64_string
    
    def create_payload(self, frame):
        """
        Tao JSON payload chua du lieu frame va metadata
        
        Tham so:
            frame: numpy array (anh OpenCV)
        Tra ve:
            Chuoi JSON voi ky tu xuong dong lam delimiter
        """
        payload = {
            "timestamp": time.time(),
            "frame_width": frame.shape[1],
            "frame_height": frame.shape[0],
            "frame_data": self.encode_frame_to_base64(frame)
        }
        # Them ky tu xuong dong lam delimiter cho Spark socket source
        return (json.dumps(payload) + "\n").encode('utf-8')
    
    def stream_video(self):
        """Vong lap chinh de thu va stream video frames"""
        # Khoi tao video capture
        cap = cv2.VideoCapture(self.video_source)
        
        if not cap.isOpened():
            print("[Camera Server] Loi: Khong the mo nguon video")
            return
        
        # Thiet lap cac thuoc tinh capture
        cap.set(cv2.CAP_PROP_FRAME_WIDTH, CameraServerConfig.FRAME_WIDTH)
        cap.set(cv2.CAP_PROP_FRAME_HEIGHT, CameraServerConfig.FRAME_HEIGHT)
        
        self.running = True
        frame_count = 0
        frame_interval = 1.0 / CameraServerConfig.FPS_LIMIT
        last_frame_time = 0
        
        print(f"[Camera Server] Bat dau stream video tai {CameraServerConfig.FPS_LIMIT} FPS...")
        print("[Camera Server] Nhan Ctrl+C de dung")
        
        try:
            while self.running:
                current_time = time.time()
                
                # Gioi han FPS
                if current_time - last_frame_time < frame_interval:
                    continue
                
                ret, frame = cap.read()
                
                if not ret:
                    # Neu file video ket thuc, bat dau lai tu dau
                    if self.video_source != 0:
                        cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
                        continue
                    else:
                        print("[Camera Server] Loi: Khong the doc frame tu webcam")
                        break
                
                # Resize frame ve kich thuoc chuan
                frame = cv2.resize(frame, (CameraServerConfig.FRAME_WIDTH, 
                                          CameraServerConfig.FRAME_HEIGHT))
                
                # Tao va gui payload
                payload = self.create_payload(frame)
                
                try:
                    self.connection.send(payload)
                    frame_count += 1
                    last_frame_time = current_time
                    
                    if frame_count % 10 == 0:
                        print(f"[Camera Server] Da gui {frame_count} frames")
                        
                except BrokenPipeError:
                    print("[Camera Server] Ket noi bi dong boi consumer")
                    break
                except ConnectionResetError:
                    print("[Camera Server] Ket noi bi reset boi consumer")
                    break
                except Exception as e:
                    print(f"[Camera Server] Loi khi gui frame: {e}")
                    break
                    
        except KeyboardInterrupt:
            print("\n[Camera Server] Dang dung...")
            
        finally:
            self.cleanup(cap)
    
    def cleanup(self, cap=None):
        """Don dep tai nguyen"""
        self.running = False
        
        if cap is not None:
            cap.release()
            
        if self.connection:
            self.connection.close()
            
        if self.server_socket:
            self.server_socket.close()
            
        print("[Camera Server] Da dong hoan tat")


def main():
    """Diem vao chinh cua chuong trinh"""
    print("=" * 60)
    print("CAMERA SERVER - Video Streaming Producer")
    print("=" * 60)
    
    # Su dung 0 cho webcam, hoac cung cap duong dan den file video
    # Vi du: video_source = "path/to/video.mp4"
    video_source = 0
    
    server = CameraServer(video_source)
    
    if server.start_server():
        server.stream_video()


if __name__ == "__main__":
    main()
