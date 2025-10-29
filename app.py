import streamlit as st
import requests
import cv2
import numpy as np
import pandas as pd
from datetime import datetime
import time
import io
from PIL import Image

# Set page config
st.set_page_config(
    page_title="Enterprise Vehicle Analytics - iVCam",
    page_icon="🚗", 
    layout="wide"
)

class VehicleAnalyticsSystem:
    def __init__(self):
        # Try multiple possible Databricks endpoints
        self.DATABRICKS_API_URLS = [
            "https://dbc-484c2988-d6e6.cloud.databricks.com/driver-proxy-api/o/0/5003",
            "https://dbc-484c2988-d6e6.cloud.databricks.com/api/2.0/serving-endpoints",
            "https://484c2988-d6e6.cloud.databricks.com/serving-endpoints"
        ]
        self.vehicles_data = []
        self.other_objects_data = []
        
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
                        # Set reasonable resolution
                        cap.set(cv2.CAP_PROP_FRAME_WIDTH, 640)
                        cap.set(cv2.CAP_PROP_FRAME_HEIGHT, 480)
                        
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
    
    def test_databricks_connection(self):
        """Test which Databricks API endpoint works"""
        test_endpoints = [
            "/health",
            "/ping", 
            "/",
            "/invocations"
        ]
        
        for api_url in self.DATABRICKS_API_URLS:
            for endpoint in test_endpoints:
                try:
                    full_url = f"{api_url}{endpoint}"
                    st.write(f"🔍 Testing: {full_url}")
                    
                    response = requests.get(full_url, timeout=10)
                    
                    if response.status_code == 200:
                        st.success(f"✅ WORKING API: {full_url}")
                        return full_url.replace(endpoint, ""), True
                    else:
                        st.warning(f"⚠️ {full_url} - Status: {response.status_code}")
                        
                except requests.exceptions.RequestException as e:
                    st.error(f"❌ Failed: {full_url} - Error: {str(e)}")
                    continue
        
        return None, False
    
    def send_to_databricks(self, image_bytes, source="ivcam"):
        """Send image to Databricks API with better error handling"""
        # First test which API works
        working_url, is_working = self.test_databricks_connection()
        
        if not working_url:
            st.error("🚫 No working Databricks API endpoint found!")
            st.info("""
            **Possible Solutions:**
            1. Check if Databricks workspace is running
            2. Verify the serving endpoint URL
            3. Check authentication tokens
            4. Ensure the model endpoint is active
            """)
            return None
            
        try:
            files = {"image": ("detection.jpg", image_bytes, "image/jpeg")}
            data = {"source": source}
            
            # Try different endpoint structures
            endpoints_to_try = [
                f"{working_url}/detect",
                f"{working_url}/predict", 
                f"{working_url}/invocations",
                f"{working_url}/api/predict"
            ]
            
            for endpoint in endpoints_to_try:
                try:
                    st.write(f"🔄 Trying endpoint: {endpoint}")
                    response = requests.post(
                        endpoint, 
                        files=files, 
                        data=data, 
                        timeout=30
                    )
                    
                    if response.status_code == 200:
                        st.success(f"✅ Success with endpoint: {endpoint}")
                        return response.json()
                    else:
                        st.warning(f"⚠️ Endpoint {endpoint} failed: {response.status_code}")
                        
                except Exception as e:
                    st.error(f"❌ Endpoint {endpoint} error: {e}")
                    continue
            
            return None
            
        except Exception as e:
            st.error(f"❌ API Connection Error: {e}")
            return None

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
    
    system = st.session_state.system
    
    # Sidebar
    with st.sidebar:
        st.header("⚙️ iVCam Settings")
        
        st.info("""
        **iVCam Status: CONNECTED** ✅
        Your phone shows: "Nothing Phone (3) Connected"
        """)
        
        if st.button("🔗 AUTO-CONNECT iVCam", type="primary"):
            with st.spinner("Scanning for iVCam..."):
                success, message, cap = system.connect_ivcam()
                if success:
                    st.session_state.ivcam_connected = True
                    st.session_state.cap = cap
                    st.success(message)
                    
                    # Show preview
                    frame = system.capture_frame(cap)
                    if frame is not None:
                        frame_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                        st.image(frame_rgb, caption="iVCam Live Preview", use_column_width=True)
                else:
                    st.error(message)
        
        st.header("📊 Controls")
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("🔍 Start Detection", type="primary"):
                st.session_state.detection_active = True
                st.rerun()
        
        with col2:
            if st.button("🛑 Stop Detection"):
                st.session_state.detection_active = False
                st.rerun()
        
        # Test API connection
        st.header("🔗 API Status")
        if st.button("🔄 Test Databricks Connection"):
            with st.spinner("Testing all possible API endpoints..."):
                working_url, is_working = system.test_databricks_connection()
                if is_working:
                    st.success(f"✅ Databricks API Connected!")
                    st.write(f"Working URL: {working_url}")
                else:
                    st.error("❌ All Databricks API endpoints failed!")
                    
                    st.info("""
                    **To Fix This:**
                    1. Go to your Databricks workspace
                    2. Check serving endpoints
                    3. Get the correct API URL
                    4. Update the code with new URL
                    """)
    
    # Main content
    tab1, tab2, tab3 = st.tabs(["🎥 Live iVCam", "📁 Image Upload", "🔧 API Setup"])
    
    with tab1:
        st.header("Live iVCam Stream")
        
        if st.session_state.ivcam_connected:
            if st.session_state.detection_active:
                # Live stream container
                stream_placeholder = st.empty()
                results_placeholder = st.empty()
                
                # Test API first
                working_url, is_working = system.test_databricks_connection()
                if not is_working:
                    st.error("🚫 Cannot start detection - No working API endpoint!")
                    st.session_state.detection_active = False
                    st.rerun()
                
                last_detection_time = 0
                frame_count = 0
                
                # Live detection loop
                while st.session_state.detection_active and st.session_state.ivcam_connected:
                    frame = system.capture_frame(st.session_state.cap)
                    if frame is not None:
                        frame_count += 1
                        
                        # Display live stream
                        frame_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                        stream_placeholder.image(frame_rgb, channels="RGB", use_column_width=True, 
                                               caption=f"iVCam Live - Frame: {frame_count}")
                        
                        # Auto-detection every 5 seconds
                        current_time = time.time()
                        if current_time - last_detection_time > 5:
                            last_detection_time = current_time
                            
                            with results_placeholder.container():
                                with st.spinner("🔄 Analyzing frame..."):
                                    # Process frame
                                    _, img_encoded = cv2.imencode('.jpg', frame)
                                    img_bytes = img_encoded.tobytes()
                                    
                                    # Send to Databricks
                                    result = system.send_to_databricks(img_bytes, "ivcam_live")
                                    
                                    if result and result.get('success'):
                                        vehicles = result['detections']['vehicles']
                                        objects = result['detections']['other_objects']
                                        
                                        system.vehicles_data.extend(vehicles)
                                        system.other_objects_data.extend(objects)
                                        
                                        if vehicles or objects:
                                            st.success(f"✅ Detected {len(vehicles)} vehicles, {len(objects)} other objects!")
                                        else:
                                            st.info("🔍 No objects detected in this frame")
                    
                    time.sleep(0.1)
            else:
                st.info("⏸️ Detection paused. Click 'Start Detection' to begin.")
        else:
            st.info("👆 Click 'AUTO-CONNECT iVCam' to start")
    
    with tab2:
        st.header("Image Upload Detection")
        
        uploaded_file = st.file_uploader("Upload image for detection", type=['jpg', 'jpeg', 'png'])
        
        if uploaded_file:
            image = Image.open(uploaded_file)
            st.image(image, caption="Uploaded Image", use_column_width=True)
            
            if st.button("🔍 Detect Objects"):
                with st.spinner("Processing image..."):
                    img_byte_arr = io.BytesIO()
                    image.save(img_byte_arr, format='JPEG')
                    img_bytes = img_byte_arr.getvalue()
                    
                    result = system.send_to_databricks(img_bytes, "manual_upload")
                    
                    if result and result.get('success'):
                        vehicles = result['detections']['vehicles']
                        objects = result['detections']['other_objects']
                        
                        st.success(f"✅ Found {len(vehicles)} vehicles, {len(objects)} other objects!")
                    else:
                        st.error("❌ Detection failed. Check API connection.")
    
    with tab3:
        st.header("API Setup Guide")
        st.info("""
        **If Databricks API is failing:**
        
        1. **Check Databricks Workspace:**
           - Go to your Databricks workspace
           - Navigate to Serving Endpoints
           - Ensure your model endpoint is running
        
        2. **Get Correct API URL:**
           - Copy the serving endpoint URL
           - It should look like: `https://xxx-yyy.cloud.databricks.com/serving-endpoints/...`
        
        3. **Update Authentication:**
           - Check if API tokens are valid
           - Update any authentication headers
        
        4. **Test Endpoints:**
           - Use the Test button above
           - Update the code with working URL
        """)
        
        # Manual API URL input
        st.subheader("Manual API URL Setup")
        custom_url = st.text_input("Enter Databricks API URL:")
        if st.button("Test Custom URL"):
            if custom_url:
                try:
                    response = requests.get(custom_url, timeout=10)
                    if response.status_code == 200:
                        st.success("✅ Custom URL works!")
                        # Update the system with new URL
                        system.DATABRICKS_API_URLS.insert(0, custom_url)
                    else:
                        st.error(f"❌ Custom URL failed: {response.status_code}")
                except Exception as e:
                    st.error(f"❌ Custom URL error: {e}")

if __name__ == "__main__":
    main()
