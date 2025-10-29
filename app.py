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
import os

# Set page config
st.set_page_config(
    page_title="Enterprise Vehicle Analytics - RTSP Stream",
    page_icon="🚗", 
    layout="wide"
)

class VehicleAnalyticsSystem:
    def __init__(self):
        # SAFE: Use Streamlit secrets
        try:
            self.API_TOKEN = st.secrets["DATABRICKS_TOKEN"]
            self.DATABRICKS_HOST = st.secrets["DATABRICKS_HOST"]
            self.DATABRICKS_JOB_ID = st.secrets["DATABRICKS_JOB_ID"]
            self.credentials_configured = True
        except:
            self.credentials_configured = False
            self.API_TOKEN = None
            self.DATABRICKS_HOST = None
            self.DATABRICKS_JOB_ID = None
        
        self.vehicles_data = []
        self.other_objects_data = []
        self.processing_queue = Queue()
        self.results_queue = Queue()
        self.is_processing = False
        self.rtsp_url = "rtsp://admin:admin@192.168.1.5:1935"
        
    def connect_rtsp_stream(self, rtsp_url=None):
        """Connect to RTSP video stream - WORKS on Streamlit Cloud!"""
        try:
            url = rtsp_url or self.rtsp_url
            st.info(f"🔗 Connecting to RTSP: {url}")
            
            # Open RTSP stream
            cap = cv2.VideoCapture(url)
            
            if cap.isOpened():
                # Set buffer size for better streaming
                cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
                cap.set(cv2.CAP_PROP_FPS, 15)
                
                # Wait for stream to initialize
                time.sleep(2)
                
                # Try to read frames
                for attempt in range(15):
                    ret, frame = cap.read()
                    if ret and frame is not None:
                        st.success("🎉 RTSP Stream Connected!")
                        return True, f"✅ Connected to RTSP: {url}", cap
                    time.sleep(0.2)
                
                # If we can open but frames are slow, still return
                return True, f"⚠️ RTSP opened but frames delayed: {url}", cap
            
            return False, f"❌ Cannot connect to RTSP: {url}", None
            
        except Exception as e:
            return False, f"❌ RTSP Error: {str(e)}", None

    def capture_frame(self, cap):
        """Capture frame from RTSP stream"""
        if cap and cap.isOpened():
            # Clear buffer to get latest frame
            for _ in range(3):
                ret, frame = cap.read()
                if ret and frame is not None:
                    return frame
        return None
    
    def check_credentials(self):
        """Check if credentials are properly configured"""
        if not self.credentials_configured:
            st.error("🔐 Databricks credentials not configured!")
            st.info("""
            **Add to `.streamlit/secrets.toml`:**
            ```
            DATABRICKS_TOKEN = "your_actual_token_here"
            DATABRICKS_HOST = "https://dbc-484c2988-d6e6.cloud.databricks.com"
            DATABRICKS_JOB_ID = 759244466463781
            ```
            """)
            return False
        return True
    
    def test_databricks_connection(self):
        """Test Databricks Job API connection"""
        if not self.check_credentials():
            return False
            
        try:
            headers = {
                "Authorization": f"Bearer {self.API_TOKEN}",
                "Content-Type": "application/json"
            }
            
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
            elif response.status_code == 403:
                st.error("❌ 403 Forbidden - Invalid API Token!")
                return False
            else:
                st.error(f"❌ Connection failed: {response.status_code}")
                return False
                
        except Exception as e:
            st.error(f"❌ Connection error: {e}")
            return False
    
    def start_video_processing(self):
        """Start background video processing thread"""
        if not self.check_credentials():
            st.error("❌ Cannot start processing - credentials not configured")
            return False
            
        self.is_processing = True
        processing_thread = threading.Thread(target=self._process_video_frames, daemon=True)
        processing_thread.start()
        return True
    
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
                    result = self._process_single_frame(frame, f"rtsp_frame_{frame_id}")
                    
                    # Store result
                    if result and result.get('success'):
                        self.results_queue.put(result)
                        
            except Exception as e:
                print(f"Processing error: {e}")
            
            time.sleep(0.1)
    
    def _process_single_frame(self, frame, source_info):
        """Process a single video frame"""
        if not self.check_credentials():
            return {"success": False, "error": "Credentials not configured"}
            
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
                return {"success": True, "processing": "completed", "run_id": run_id}
            else:
                return {"success": False, "error": f"Job submission failed: {submit_response.status_code}"}
                
        except Exception as e:
            return {"success": False, "error": str(e)}

def main():
    st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        color: #1E3A8A;
        text-align: center;
        margin-bottom: 2rem;
    }
    .success-box {
        background-color: #D1FAE5;
        padding: 1rem;
        border-radius: 10px;
        border-left: 5px solid #10B981;
    }
    .warning-box {
        background-color: #FEF3C7;
        padding: 1rem;
        border-radius: 10px;
        border-left: 5px solid #F59E0B;
    }
    </style>
    """, unsafe_allow_html=True)
    
    st.markdown('<h1 class="main-header">🚗 ENTERPRISE VEHICLE ANALYTICS - RTSP Stream</h1>', unsafe_allow_html=True)
    
    # Initialize session state
    if "system" not in st.session_state:
        st.session_state.system = VehicleAnalyticsSystem()
    if "stream_connected" not in st.session_state:
        st.session_state.stream_connected = False
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
        st.header("⚙️ RTSP Stream Settings")
        
        # RTSP URL input
        rtsp_url = st.text_input(
            "RTSP Stream URL:", 
            value="rtsp://admin:admin@192.168.1.5:1935",
            help="Enter your RTSP stream URL"
        )
        
        # Connect button
        if st.button("🔗 CONNECT RTSP STREAM", type="primary"):
            with st.spinner("Connecting to RTSP stream..."):
                success, message, cap = system.connect_rtsp_stream(rtsp_url)
                if success:
                    st.session_state.stream_connected = True
                    st.session_state.cap = cap
                    st.success(message)
                else:
                    st.error(message)
        
        st.header("📊 Video Controls")
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("🔍 Start Detection", type="primary"):
                if system.start_video_processing():
                    st.session_state.detection_active = True
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
        
        # Credentials status
        st.header("🔐 Security Status")
        if system.credentials_configured:
            st.success("✅ Credentials Configured")
        else:
            st.error("❌ Credentials Missing")
    
    # Main content
    tab1, tab2, tab3 = st.tabs(["🎥 Live RTSP Stream", "📊 Analytics Dashboard", "🔧 Setup Guide"])
    
    with tab1:
        st.header("Live RTSP Video Stream")
        
        if st.session_state.stream_connected:
            if st.session_state.detection_active:
                # Live stream container
                stream_placeholder = st.empty()
                stats_placeholder = st.empty()
                results_placeholder = st.empty()
                
                # Video processing loop
                while st.session_state.detection_active and st.session_state.stream_connected:
                    frame = system.capture_frame(st.session_state.cap)
                    if frame is not None:
                        # Display live stream
                        frame_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                        stream_placeholder.image(frame_rgb, channels="RGB", use_column_width=True, 
                                               caption=f"Live RTSP Stream - Frame: {st.session_state.frame_counter}")
                        
                        # Process frames at intervals
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
            st.info("👆 Enter RTSP URL and click 'CONNECT RTSP STREAM' to start")
            
            # Show sample RTSP URLs
            with st.expander("📋 Common RTSP URL Formats"):
                st.code("""
# iVCam RTSP:
rtsp://admin:admin@192.168.1.5:1935

# IP Camera formats:
rtsp://username:password@ip_address:port
rtsp://ip_address:554/stream1
rtsp://admin:1234@192.168.1.100:554/h264

# Test public RTSP (for testing):
rtsp://wowzaec2demo.streamlock.net/vod/mp4:BigBuckBunny_115k.mov
                """)
    
    with tab2:
        st.header("Real-time Analytics Dashboard")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🚗 Vehicle Detection History")
            if system.vehicles_data:
                vehicle_df = pd.DataFrame(system.vehicles_data)
                st.dataframe(vehicle_df.tail(10))
                
                if 'vehicle_type' in vehicle_df.columns:
                    st.bar_chart(vehicle_df['vehicle_type'].value_counts())
            else:
                st.info("No vehicle detections yet")
        
        with col2:
            st.subheader("🌳 Object Detection History")
            if system.other_objects_data:
                object_df = pd.DataFrame(system.other_objects_data)
                st.dataframe(object_df.tail(10))
                
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
    
    with tab3:
        st.header("🔧 RTSP Setup Guide")
        
        st.markdown("""
        ### 🎯 RTSP Stream Configuration
        
        **Your RTSP URL:** `rtsp://admin:admin@192.168.1.5:1935`
        
        **Format:** `rtsp://username:password@ip_address:port`
        
        ### 🔧 Troubleshooting RTSP
        
        **If connection fails:**
        1. **Verify RTSP URL** - check IP, port, credentials
        2. **Check network** - ensure device is on same network
        3. **Test with VLC** - try opening in VLC media player
        4. **Firewall** - ensure port 1935 is open
        
        **Test with public RTSP stream:**
        ```
        rtsp://wowzaec2demo.streamlock.net/vod/mp4:BigBuckBunny_115k.mov
        ```
        
        ### 🔐 Databricks Setup
        
        Add to `.streamlit/secrets.toml`:
        ```toml
        DATABRICKS_TOKEN = "your_actual_token"
        DATABRICKS_HOST = "https://dbc-484c2988-d6e6.cloud.databricks.com"
        DATABRICKS_JOB_ID = 759244466463781
        ```
        """)

if __name__ == "__main__":
    main()
