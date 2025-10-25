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
    page_title="Enterprise Vehicle Analytics - IVCam",
    page_icon="🚗", 
    layout="wide"
)

# Custom CSS
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
</style>
""", unsafe_allow_html=True)

class VehicleAnalyticsSystem:
    def __init__(self):
        # Your Databricks API URL
        self.DATABRICKS_API_URL = "https://dbc-484c2988-d6e6.cloud.databricks.com/driver-proxy-api/o/0/5000"
        self.vehicles_data = []
        self.other_objects_data = []
        
    def connect_ivcam(self, camera_url):
        """Connect to IVCam stream"""
        try:
            cap = cv2.VideoCapture(camera_url)
            if cap.isOpened():
                return True, "✅ IVCam Connected Successfully!", cap
            return False, "❌ Failed to connect to IVCam", None
        except Exception as e:
            return False, f"❌ IVCam Error: {e}", None
    
    def capture_frame(self, cap):
        """Capture frame from IVCam"""
        if cap and cap.isOpened():
            ret, frame = cap.read()
            return frame if ret else None
        return None
    
    def send_to_databricks(self, image_bytes, source="web_upload"):
        """Send image to Databricks API"""
        try:
            files = {"image": ("detection.jpg", image_bytes, "image/jpeg")}
            data = {"source": source}
            response = requests.post(
                f"{self.DATABRICKS_API_URL}/detect", 
                files=files, 
                data=data, 
                timeout=30
            )
            return response.json() if response.status_code == 200 else None
        except Exception as e:
            st.error(f"API Connection Error: {e}")
            return None
    
    def export_from_databricks(self):
        """Export CSV data from Databricks"""
        try:
            response = requests.get(f"{self.DATABRICKS_API_URL}/export", timeout=30)
            return response.json() if response.status_code == 200 else None
        except:
            return None

def main():
    st.markdown('<h1 class="main-header">🚗 ENTERPRISE VEHICLE ANALYTICS</h1>', unsafe_allow_html=True)
    
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
        st.header("⚙️ IVCam Settings")
        camera_url = st.text_input("IVCam Stream URL", "http://192.168.1.5:8080/video")
        
        if st.button("🔗 Connect IVCam", type="primary"):
            with st.spinner("Connecting to IVCam..."):
                success, message, cap = system.connect_ivcam(camera_url)
                if success:
                    st.session_state.ivcam_connected = True
                    st.session_state.cap = cap
                    st.success(message)
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
        
        st.header("💾 Data Management")
        if st.button("🗑️ Clear Session Data"):
            system.vehicles_data.clear()
            system.other_objects_data.clear()
            st.success("Session data cleared!")
            st.rerun()
        
        st.header("📈 Session Stats")
        st.write(f"**Vehicles Detected:** {len(system.vehicles_data)}")
        st.write(f"**Other Objects:** {len(system.other_objects_data)}")
        
        # Test API connection
        st.header("🔗 API Status")
        if st.button("🔄 Test Databricks Connection"):
            try:
                response = requests.get(f"{system.DATABRICKS_API_URL}/health", timeout=10)
                if response.status_code == 200:
                    health_data = response.json()
                    st.success("✅ Databricks API Connected!")
                    st.write(f"Detections Processed: {health_data.get('detections_processed', 0)}")
                else:
                    st.error("❌ Databricks API Connection Failed")
            except Exception as e:
                st.error(f"❌ Connection Error: {e}")
    
    # Main content - Tabs
    tab1, tab2, tab3 = st.tabs(["🎥 Live IVCam", "📊 Analytics", "📁 Image Upload"])
    
    with tab1:
        st.header("Live IVCam Stream")
        
        if st.session_state.ivcam_connected:
            # Live stream settings
            refresh_rate = st.slider("Detection Frequency (seconds)", 1, 10, 3)
            
            if st.session_state.detection_active:
                # Live stream container
                stream_placeholder = st.empty()
                results_placeholder = st.empty()
                
                last_detection_time = 0
                
                # Live detection loop
                while st.session_state.detection_active and st.session_state.ivcam_connected:
                    frame = system.capture_frame(st.session_state.cap)
                    if frame is not None:
                        # Display live stream
                        frame_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                        stream_placeholder.image(frame_rgb, channels="RGB", use_column_width=True, caption="Live IVCam Stream")
                        
                        # Auto-detection
                        current_time = time.time()
                        if current_time - last_detection_time > refresh_rate:
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
                                            
                                            # Show vehicles
                                            if vehicles:
                                                with st.expander(f"🚗 Vehicles ({len(vehicles)})"):
                                                    for i, vehicle in enumerate(vehicles):
                                                        col1, col2 = st.columns(2)
                                                        with col1:
                                                            st.write(f"**Type:** {vehicle.get('vehicle_type', 'Unknown')}")
                                                            st.write(f"**Confidence:** {vehicle.get('confidence', 0):.2%}")
                                                        with col2:
                                                            st.write(f"**Color:** {vehicle.get('color', 'Unknown')}")
                                                            st.write(f"**License:** {vehicle.get('license_plate', 'Not detected')}")
                                                        if i < len(vehicles) - 1:
                                                            st.divider()
                                            
                                            # Show objects
                                            if objects:
                                                with st.expander(f"🌳 Other Objects ({len(objects)})"):
                                                    for i, obj in enumerate(objects):
                                                        col1, col2 = st.columns(2)
                                                        with col1:
                                                            st.write(f"**Type:** {obj.get('object_type', 'Unknown')}")
                                                            st.write(f"**Confidence:** {obj.get('confidence', 0):.2%}")
                                                        with col2:
                                                            st.write(f"**Location:** {obj.get('location', 'Unknown')}")
                                                            st.write(f"**Size:** {obj.get('size_category', 'Unknown')}")
                                                        if i < len(objects) - 1:
                                                            st.divider()
                    
                    time.sleep(0.1)
            else:
                st.info("⏸️ Detection paused. Click 'Start Detection' to begin real-time analysis.")
        else:
            st.info("👆 Connect IVCam to start live detection")
    
    with tab2:
        st.header("Analytics Dashboard")
        
        # Export data from Databricks
        if st.button("🔄 Load Latest Data from Databricks"):
            with st.spinner("Loading data from Databricks engine..."):
                export_result = system.export_from_databricks()
                
                if export_result and export_result.get('success'):
                    # Vehicles CSV
                    vehicles_csv = export_result.get('vehicles_csv', '')
                    if vehicles_csv:
                        try:
                            vehicles_df = pd.read_csv(io.StringIO(vehicles_csv))
                            st.subheader("🚗 Vehicles Data")
                            
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                st.metric("Total Vehicles", len(vehicles_df))
                            with col2:
                                st.metric("Vehicle Types", vehicles_df['vehicle_type'].nunique())
                            with col3:
                                avg_conf = vehicles_df['confidence'].mean()
                                st.metric("Avg Confidence", f"{avg_conf:.2%}")
                            
                            # Download button
                            st.download_button(
                                "📥 Download Vehicles CSV",
                                vehicles_csv,
                                f"vehicles_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                "text/csv"
                            )
                            
                            # Show data table
                            with st.expander("View Vehicles Data Table"):
                                st.dataframe(vehicles_df)
                                
                        except Exception as e:
                            st.error(f"Error processing vehicles data: {e}")
                    
                    # Other Objects CSV
                    others_csv = export_result.get('other_objects_csv', '')
                    if others_csv:
                        try:
                            others_df = pd.read_csv(io.StringIO(others_csv))
                            st.subheader("🌳 Other Objects Data")
                            
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                st.metric("Total Objects", len(others_df))
                            with col2:
                                st.metric("Object Types", others_df['object_type'].nunique())
                            with col3:
                                avg_conf = others_df['confidence'].mean()
                                st.metric("Avg Confidence", f"{avg_conf:.2%}")
                            
                            # Download button
                            st.download_button(
                                "📥 Download Objects CSV",
                                others_csv,
                                f"objects_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                "text/csv"
                            )
                            
                            # Show data table
                            with st.expander("View Objects Data Table"):
                                st.dataframe(others_df)
                                
                        except Exception as e:
                            st.error(f"Error processing objects data: {e}")
                else:
                    st.info("📊 No data available from Databricks yet. Process some images first.")
        
        # Session data summary
        if system.vehicles_data or system.other_objects_data:
            st.subheader("📋 Current Session Summary")
            col1, col2 = st.columns(2)
            
            with col1:
                st.metric("Session Vehicles", len(system.vehicles_data))
                if system.vehicles_data:
                    vehicle_df = pd.DataFrame(system.vehicles_data)
                    st.write("Vehicle Distribution:")
                    st.bar_chart(vehicle_df['vehicle_type'].value_counts())
            
            with col2:
                st.metric("Session Objects", len(system.other_objects_data))
                if system.other_objects_data:
                    object_df = pd.DataFrame(system.other_objects_data)
                    st.write("Object Distribution:")
                    st.bar_chart(object_df['object_type'].value_counts())
    
    with tab3:
        st.header("Image Upload Detection")
        
        uploaded_file = st.file_uploader("Upload image for detection", type=['jpg', 'jpeg', 'png'])
        
        if uploaded_file:
            image = Image.open(uploaded_file)
            st.image(image, caption="Uploaded Image", use_column_width=True)
            
            if st.button("🔍 Detect Objects in Image"):
                with st.spinner("Processing image through AI pipeline..."):
                    # Convert to bytes
                    img_byte_arr = io.BytesIO()
                    image.save(img_byte_arr, format='JPEG')
                    img_bytes = img_byte_arr.getvalue()
                    
                    # Send to Databricks
                    result = system.send_to_databricks(img_bytes, "manual_upload")
                    
                    if result and result.get('success'):
                        vehicles = result['detections']['vehicles']
                        objects = result['detections']['other_objects']
                        
                        system.vehicles_data.extend(vehicles)
                        system.other_objects_data.extend(objects)
                        
                        st.success(f"✅ Found {len(vehicles)} vehicles, {len(objects)} other objects!")
                        
                        # Show vehicles
                        if vehicles:
                            with st.expander(f"🚗 Vehicles ({len(vehicles)})"):
                                for i, vehicle in enumerate(vehicles):
                                    col1, col2 = st.columns(2)
                                    with col1:
                                        st.write(f"**Type:** {vehicle.get('vehicle_type', 'Unknown')}")
                                        st.write(f"**Confidence:** {vehicle.get('confidence', 0):.2%}")
                                    with col2:
                                        st.write(f"**Color:** {vehicle.get('color', 'Unknown')}")
                                        st.write(f"**License:** {vehicle.get('license_plate', 'Not detected')}")
                                    if i < len(vehicles) - 1:
                                        st.divider()
                        
                        # Show objects
                        if objects:
                            with st.expander(f"🌳 Other Objects ({len(objects)})"):
                                for i, obj in enumerate(objects):
                                    col1, col2 = st.columns(2)
                                    with col1:
                                        st.write(f"**Type:** {obj.get('object_type', 'Unknown')}")
                                        st.write(f"**Confidence:** {obj.get('confidence', 0):.2%}")
                                    with col2:
                                        st.write(f"**Location:** {obj.get('location', 'Unknown')}")
                                        st.write(f"**Size:** {obj.get('size_category', 'Unknown')}")
                                    if i < len(objects) - 1:
                                        st.divider()

if __name__ == "__main__":
    main()
