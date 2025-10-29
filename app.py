import streamlit as st
import requests
import cv2
import numpy as np
import pandas as pd
from datetime import datetime
import time
import io
from PIL import Image
import base64
import json
import threading
from queue import Queue

# Set page config
st.set_page_config(
    page_title="Enterprise Vehicle Analytics - iVCam",
    page_icon="🚗", 
    layout="wide"
)

class VehicleAnalyticsSystem:
    def __init__(self):
        self.DATABRICKS_JOB_ID = 759244466463781  # Your job ID
        self.DATABRICKS_HOST = "https://dbc-484c2988-d6e6.cloud.databricks.com"
        self.API_TOKEN = "dapiyour_token_here"  # 🔴 REPLACE WITH YOUR TOKEN
        
        self.vehicles_data = []
        self.other_objects_data = []
        self.processing_queue = Queue()
        self.results_queue = Queue()
        self.is_processing = False
        
    def connect_ivcam(self):
        """SPECIAL iVCam connection that WORKS"""
        try:
            # iVCam USUALLY appears as camera index 1 on Windows
            # Try multiple indexes with DirectShow
            for camera_index in [1, 0, 2, 3]:
                try:
                    # MUST use DirectShow backend for iVCam
                    cap = cv2.VideoCapture(camera_index, cv2.CAP_DSHOW)
                    
                    if cap.isOpened():
                        # Set reasonable resolution for better performance
                        cap.set(cv2.CAP_PROP_FRAME_WIDTH, 640)
                        cap.set(cv2.CAP_PROP_FRAME_HEIGHT, 480)
                        cap.set(cv2.CAP_PROP_FPS, 15)  # Limit FPS for stability
                        
                        # Try to read a frame (iVCam can be slow)
                        for attempt in range(10):
                            ret, frame = cap.read()
                            if ret and frame is not None:
                                st.success(f"✅ iVCam FOUND at index {camera_index}!")
                                return True, f"✅ iVCam Connected (Index: {camera_index})", cap
                            time.sleep(0.1)
                        
                        cap.release()
                except Exception as e:
                    continue
            
            return False, "❌ iVCam not found. Try USB connection.", None
            
        except Exception as e:
            return False, f"❌ Error: {e}", None
    
    def capture_frame(self, cap):
        """Capture frame from iVCam"""
        if cap and cap.isOpened():
            ret, frame = cap.read()
            if ret:
                return frame
        return None
    
    def start_video_processing(self):
        """Start background video processing thread"""
        self.is_processing = True
        processing_thread = threading.Thread(target=self._process_video_frames, daemon=True)
        processing_thread.start()
    
    def stop_video_processing(self):
        """Stop video processing"""
        self.is_processing = False
    
    def _process_video_frames(self):
        """Background thread for processing video frames"""
        while self.is_processing:
            try:
                # Get frame from queue (if any)
                if not self.processing_queue.empty():
                    frame_data = self.processing_queue.get()
                    frame, frame_id = frame_data
                    
                    # Process frame through Databricks
                    result = self._process_single_frame(frame, f"ivcam_frame_{frame_id}")
                    
                    # Store result
                    if result and result.get('success'):
                        self.results_queue.put(result)
                        
            except Exception as e:
                print(f"Processing error: {e}")
            
            time.sleep(0.1)  # Prevent CPU overload
    
    def _process_single_frame(self, frame, source_info):
        """Process a single video frame"""
        try:
            # Encode frame to JPEG
            _, img_encoded = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 80])
            img_bytes = img_encoded.tobytes()
            
            # Convert to base64
            image_b64 = base64.b64encode(img_bytes).decode('utf-8')
            
            # Job payload
            job_payload = {
                "job_id": self.DATABRICKS_JOB_ID,
                "notebook_params": {
                    "image_base64": image_b64,
                    "source_info": source_info
                }
            }
            
            headers = {
                "Authorization": f"Bearer {self.API_TOKEN}",
                "Content-Type": "application/json"
            }
            
            # Submit job run
            submit_response = requests.post(
                f"{self.DATABRICKS_HOST}/api/2.1/jobs/run-now",
                json=job_payload,
                headers=headers,
                timeout=30
            )
            
            if submit_response.status_code == 200:
                run_id = submit_response.json()["run_id"]
                
                # Wait for job completion (simplified - no long wait for video)
                return self._quick_job_check(run_id)
            else:
                return {"success": False, "error": f"Job submission failed: {submit_response.status_code}"}
                
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def _quick_job_check(self, run_id, wait_time=5):
        """Quick check for job status (optimized for video)"""
        headers = {"Authorization": f"Bearer {self.API_TOKEN}"}
        
        time.sleep(wait_time)  # Brief wait
        
        try:
            status_response = requests.get(
                f"{self.DATABRICKS_HOST}/api/2.1/jobs/runs/get?run_id={run_id}",
                headers=headers,
                timeout=10
            )
            
            if status_response.status_code == 200:
                status_data = status_response.json()
                state = status_data["state"]
                
                if state["life_cycle_state"] == "TERMINATED" and state["result_state"] == "SUCCESS":
                    # Get output if available
                    output_response = requests.get(
                        f"{self.DATABRICKS_HOST}/api/2.1/jobs/runs/get-output?run_id={run_id}",
                        headers=headers,
                        timeout=10
                    )
                    
                    if output_response.status_code == 200:
                        output_data = output_response.json()
                        return output_data.get("notebook_output", {}).get("result", {"success": True})
                
                # Return basic success for video (don't wait too long)
                return {"success": True, "processing": "completed", "run_id": run_id}
            
        except Exception as e:
            print(f"Job check error: {e}")
        
        return {"success": True, "processing": "in_progress", "run_id": run_id}
    
    def test_databricks_connection(self):
        """Test Databricks Job API connection"""
        try:
            headers = {
                "Authorization": f"Bearer {self.API_TOKEN}",
                "Content-Type": "application/json"
            }
            
            # Test by getting job info
            response = requests.get(
                f"{self.DATABRICKS_HOST}/api/2.1/jobs/get?job_id={self.DATABRICKS_JOB_ID}",
                headers=headers,
                timeout=10
            )
            
            if response.status_code == 200:
                job_info = response.json()
                st.success(f"✅ Databricks Job Connected!")
                st.write(f"Job Name: {job_info.get('settings', {}).get('name', 'Unknown')}")
                return True
            else:
                st.error(f"❌ Job connection failed: {response.status_code}")
                return False
                
        except Exception as e:
            st.error(f"❌ Connection error: {e}")
            return False

def main():
    st.markdown('<h1 class="main-header">🚗 ENTERPRISE VEHICLE ANALYTICS - iVCam</h1>', unsafe_allow_html=True)
    
    # Initialize session state
    if "system" not in st.session_state:
        st.session_state.system = VehicleAnalyticsSystem()
    if "ivcam_connected" not in st.session_state:
        st.session_state.ivcam_connected = False
    if "detection_active" not in st.session_state:
        st.session_state.detection_active = False
    if "cap" not in st.session_state:
        st.session_state.cap = None
    if "frame_counter" not in st.session_state:
        st.session_state.frame_counter = 0
    if "last_processed_time" not in st.session_state:
        st.session_state.last_processed_time = 0
    
    system = st.session_state.system
    
    # Sidebar
    with st.sidebar:
        st.header("⚙️ iVCam Settings")
        
        if st.button("🔗 AUTO-CONNECT iVCam", type="primary"):
            with st.spinner("Scanning for iVCam..."):
                success, message, cap = system.connect_ivcam()
                if success:
                    st.session_state.ivcam_connected = True
                    st.session_state.cap = cap
                    st.success(message)
                else:
                    st.error(message)
        
        st.header("📊 Video Controls")
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("🔍 Start Detection", type="primary"):
                st.session_state.detection_active = True
                system.start_video_processing()
                st.rerun()
        
        with col2:
            if st.button("🛑 Stop Detection"):
                st.session_state.detection_active = False
                system.stop_video_processing()
                st.rerun()
        
        # Processing settings
        st.header("🎯 Processing Settings")
        processing_interval = st.slider(
            "Frame Processing Interval (seconds)", 
            min_value=1, max_value=10, value=3
        )
        
        # Test API connection
        st.header("🔗 API Status")
        if st.button("🔄 Test Databricks Connection"):
            with st.spinner("Testing Databricks Job..."):
                system.test_databricks_connection()
        
        # Stats
        st.header("📈 Live Stats")
        st.write(f"Frames Processed: {st.session_state.frame_counter}")
        st.write(f"Vehicles Detected: {len(system.vehicles_data)}")
        st.write(f"Objects Detected: {len(system.other_objects_data)}")
    
    # Main content
    tab1, tab2 = st.tabs(["🎥 Live iVCam Stream", "📊 Analytics Dashboard"])
    
    with tab1:
        st.header("Live iVCam Video Stream")
        
        if st.session_state.ivcam_connected:
            if st.session_state.detection_active:
                # Live stream container
                stream_placeholder = st.empty()
                stats_placeholder = st.empty()
                results_placeholder = st.empty()
                
                # Video processing loop
                while st.session_state.detection_active and st.session_state.ivcam_connected:
                    frame = system.capture_frame(st.session_state.cap)
                    if frame is not None:
                        # Display live stream
                        frame_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                        stream_placeholder.image(frame_rgb, channels="RGB", use_column_width=True, 
                                               caption=f"Live iVCam Stream - Frame: {st.session_state.frame_counter}")
                        
                        # Process frames at intervals (for performance)
                        current_time = time.time()
                        if current_time - st.session_state.last_processed_time > processing_interval:
                            st.session_state.last_processed_time = current_time
                            st.session_state.frame_counter += 1
                            
                            # Add frame to processing queue
                            system.processing_queue.put((frame, st.session_state.frame_counter))
                            
                            # Update stats
                            with stats_placeholder.container():
                                st.info(f"📊 Frame #{st.session_state.frame_counter} queued for processing")
                        
                        # Check for new results
                        if not system.results_queue.empty():
                            result = system.results_queue.get()
                            if result and result.get('success'):
                                with results_placeholder.container():
                                    detections = result.get('detections', {})
                                    vehicles = detections.get('vehicles', [])
                                    objects = detections.get('other_objects', [])
                                    
                                    system.vehicles_data.extend(vehicles)
                                    system.other_objects_data.extend(objects)
                                    
                                    if vehicles or objects:
                                        st.success(f"🎯 Detected {len(vehicles)} vehicles, {len(objects)} objects!")
                                    else:
                                        st.info("🔍 No objects in this frame")
                    
                    time.sleep(0.03)  # ~30 FPS display
            else:
                st.info("⏸️ Detection paused. Click 'Start Detection' to begin real-time video analysis.")
        else:
            st.info("👆 Click 'AUTO-CONNECT iVCam' to start live video stream")
    
    with tab2:
        st.header("Real-time Analytics Dashboard")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🚗 Vehicle Detection History")
            if system.vehicles_data:
                vehicle_df = pd.DataFrame(system.vehicles_data)
                st.dataframe(vehicle_df.tail(10))  # Show last 10 detections
                
                # Vehicle type distribution
                if 'vehicle_type' in vehicle_df.columns:
                    st.bar_chart(vehicle_df['vehicle_type'].value_counts())
            else:
                st.info("No vehicle detections yet")
        
        with col2:
            st.subheader("🌳 Object Detection History")
            if system.other_objects_data:
                object_df = pd.DataFrame(system.other_objects_data)
                st.dataframe(object_df.tail(10))  # Show last 10 detections
                
                # Object type distribution
                if 'object_type' in object_df.columns:
                    st.bar_chart(object_df['object_type'].value_counts())
            else:
                st.info("No object detections yet")
        
        # Export data
        if st.button("📥 Export Detection Data"):
            if system.vehicles_data or system.other_objects_data:
                vehicles_csv = pd.DataFrame(system.vehicles_data).to_csv(index=False) if system.vehicles_data else ""
                objects_csv = pd.DataFrame(system.other_objects_data).to_csv(index=False) if system.other_objects_data else ""
                
                st.download_button(
                    "Download Vehicles CSV",
                    vehicles_csv,
                    f"vehicles_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    "text/csv"
                )
                
                st.download_button(
                    "Download Objects CSV", 
                    objects_csv,
                    f"objects_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    "text/csv"
                )
            else:
                st.warning("No data to export yet")

if __name__ == "__main__":
    main()
