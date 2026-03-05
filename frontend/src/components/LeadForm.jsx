import React, { useState } from 'react';
import { Lock, CheckCircle, Smartphone, Mail, User, X } from 'lucide-react';
import axios from 'axios';

const LeadForm = ({ analysisData, imageBlob, onSubmitSuccess, onCancel }) => {
    const [formData, setFormData] = useState({
        first_name: '',
        last_name: '',
        age: '',
        gender: '',
        email: '',
        phone: '',
        city: '',
        zip_code: '',
        wants_assessment: false
    });
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);

    const validateForm = () => {
        // Email validation
        const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
        if (!emailRegex.test(formData.email)) {
            setError("Please enter a valid email address.");
            return false;
        }

        // Phone validation
        const cleanPhone = formData.phone.replace(/\D/g, '');

        if (cleanPhone.length !== 10) {
            setError("Phone number must be exactly 10 digits.");
            return false;
        }

        if (cleanPhone.startsWith('1')) {
            setError("Phone number cannot start with 1.");
            return false;
        }

        // Zip Code Format Validation
        const zipRegex = /^\d{5}$/;
        if (!zipRegex.test(formData.zip_code)) {
            setError("Zip Code must be exactly 5 digits.");
            return false;
        }

        return true;
    };

    // Campaign Code Logic
    const TARGET_CITIES = {
        'Boston': { code: '#BOIG2', lat: 42.3601, lon: -71.0589 },
        'New York': { code: '#NYIG2', lat: 40.7128, lon: -74.0060 },
        'Dallas': { code: '#DAL3DE', lat: 32.7767, lon: -96.7970 },
        'Houston': { code: '#HOU3DE', lat: 29.7604, lon: -95.3698 },
        'Nashville': { code: '#NAIG2', lat: 36.1627, lon: -86.7816 },
        'Miami': { code: '#FL4IG3', lat: 25.7617, lon: -80.1918 },
        'Chicago': { code: '#CHIIG2', lat: 41.8781, lon: -87.6298 },
        'Orlando': { code: '#ORL4IG3', lat: 28.5383, lon: -81.3792 }
    };

    const getDistanceFromLatLonInKm = (lat1, lon1, lat2, lon2) => {
        const R = 6371; // Radius of the earth in km
        const dLat = (lat2 - lat1) * (Math.PI / 180);
        const dLon = (lon2 - lon1) * (Math.PI / 180);
        const a =
            Math.sin(dLat / 2) * Math.sin(dLat / 2) +
            Math.cos(lat1 * (Math.PI / 180)) * Math.cos(lat2 * (Math.PI / 180)) *
            Math.sin(dLon / 2) * Math.sin(dLon / 2);
        const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return R * c;
    }

    const calculateCampaignCode = async (zip, age, gender) => {
        try {
            // 1. Get Lat/Lon and City from Zip
            const response = await axios.get(`https://api.zippopotam.us/us/${zip}`);
            const place = response.data.places[0];
            const userLat = parseFloat(place.latitude);
            const userLon = parseFloat(place.longitude);
            const cityName = place['place name'];
            const state = place['state abbreviation'];

            // 2. State Override: Force Boston for New England states
            const bostonStates = ['CT', 'MA', 'NH', 'RI'];
            let cityCode;

            if (bostonStates.includes(state)) {
                cityCode = '#BOIG2';
            } else {
                // 3. Find Nearest City by distance
                let nearestCity = null;
                let minDistance = Infinity;

                for (const [city, data] of Object.entries(TARGET_CITIES)) {
                    const distance = getDistanceFromLatLonInKm(userLat, userLon, data.lat, data.lon);
                    if (distance < minDistance) {
                        minDistance = distance;
                        nearestCity = data.code;
                    }
                }

                cityCode = nearestCity || '#NYIG2';
            }

            // 3. Age Code
            const ageNum = parseInt(age);
            let ageCode = '1';
            if (ageNum >= 35 && ageNum <= 44) ageCode = '2';
            if (ageNum >= 45) ageCode = '3';

            // 4. Gender Code
            const genderCode = gender === 'Female' ? 'F' : 'M';

            return { code: `${cityCode}${ageCode}${genderCode}`, city: cityName };

        } catch (error) {
            console.error("Error calculating campaign code:", error);
            // Throw error to be caught by handleSubmit
            throw new Error("Invalid Zip Code");
        }
    };

    const handleSubmit = async (e) => {
        e.preventDefault();
        setLoading(true);
        setError(null);

        if (!validateForm()) {
            setLoading(false);
            return;
        }

        try {
            // Calculate Campaign Code & Detect City
            const { code: campaignCode, city: detectedCity } = await calculateCampaignCode(
                formData.zip_code,
                formData.age,
                formData.gender
            );
            console.log("Generated Campaign Code:", campaignCode, "City:", detectedCity);

            // 1. Send data to backend as FormData
            const payload = new FormData();

            // Append simple fields
            Object.keys(formData).forEach(key => {
                if (key !== 'city') { // Skip manual city
                    payload.append(key, formData[key]);
                }
            });

            // Append Auto-Detected City
            payload.append('city', detectedCity);

            // Append Calculated Campaign
            payload.append('campaign', campaignCode);

            // Append Analysis Data as JSON string
            payload.append('analysis_data', JSON.stringify(analysisData));

            // Append Image File
            if (imageBlob) {
                payload.append('file', imageBlob);
            }

            const API_URL = import.meta.env.MODE === 'production' ? '/api' : 'http://localhost:8000';
            const response = await axios.post(`${API_URL}/lead`, payload, {
                headers: { 'Content-Type': 'multipart/form-data' }
            });

            if (response.data.status === 'success') {
                console.log("Form submission success. Tracking Lead event...");
                // Track Meta Pixel Lead Event
                if (window.fbq) {
                    try {
                        window.fbq('track', 'Lead');
                        console.log("Meta Pixel 'Lead' event fired.");
                    } catch (e) {
                        console.error("Meta Pixel tracking failed:", e);
                    }
                } else {
                    console.warn("window.fbq is not defined. Pixel not tracking.");
                }

                // Small delay to ensure tracking fires before component unmount
                setTimeout(() => {
                    onSubmitSuccess();
                }, 500);
            }
        } catch (err) {
            console.error(err);
            if (err.message === "Invalid Zip Code") {
                setError("Invalid Zip Code. Please enter a valid US Zip Code.");
            } else if (err.response && err.response.data && err.response.data.message) {
                setError(err.response.data.message);
            } else {
                setError("Something went wrong. Please try again.");
            }
        } finally {
            setLoading(false);
        }
    };

    return (
        <div className="fixed inset-0 z-[100] bg-black/50 backdrop-blur-sm overflow-y-auto">
            <div className="min-h-full flex items-center justify-center p-4">
                <div className="glass-panel w-full max-w-lg p-6 sm:p-8 rounded-2xl relative bg-pastel-card border border-white/10 shadow-2xl">

                    {/* Close Button - Sticky to verify visibility or just absolute in relative container */}
                    <button
                        onClick={onCancel}
                        className="absolute top-4 right-4 p-2 text-pastel-muted hover:text-white transition-colors rounded-full hover:bg-white/10 z-10"
                        aria-label="Close"
                    >
                        <X size={24} />
                    </button>

                    <div className="text-center mb-6">
                        <div className="inline-flex items-center justify-center w-12 h-12 rounded-full bg-pastel-accent/10 mb-4">
                            <User className="text-pastel-accent" size={24} />
                        </div>
                        <h2 className="text-2xl font-bold text-white mb-2">Claim Your Spot</h2>
                        <p className="text-pastel-muted text-sm">
                            Enter your details below if you'd like to start modeling
                        </p>
                    </div>

                    <form onSubmit={handleSubmit} className="space-y-4">
                        <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                            <div className="space-y-2">
                                <label className="text-xs uppercase tracking-wider text-pastel-muted font-semibold">First Name *</label>
                                <input
                                    type="text"
                                    required
                                    className="w-full bg-white/5 border border-white/10 rounded-lg py-3.5 px-4 text-pastel-text text-base focus:outline-none focus:border-pastel-accent transition-colors"
                                    placeholder="Jane"
                                    value={formData.first_name}
                                    onChange={e => setFormData({ ...formData, first_name: e.target.value })}
                                />
                            </div>
                            <div className="space-y-2">
                                <label className="text-xs uppercase tracking-wider text-pastel-muted font-semibold">Last Name *</label>
                                <input
                                    type="text"
                                    required
                                    className="w-full bg-white/5 border border-white/10 rounded-lg py-3.5 px-4 text-pastel-text text-base focus:outline-none focus:border-pastel-accent transition-colors"
                                    placeholder="Doe"
                                    value={formData.last_name}
                                    onChange={e => setFormData({ ...formData, last_name: e.target.value })}
                                />
                            </div>
                        </div>

                        <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                            <div className="space-y-2">
                                <label className="text-xs uppercase tracking-wider text-pastel-muted font-semibold">Age *</label>
                                <input
                                    type="number"
                                    required
                                    className="w-full bg-white/5 border border-white/10 rounded-lg py-3.5 px-4 text-pastel-text text-base focus:outline-none focus:border-pastel-accent transition-colors"
                                    placeholder="25"
                                    value={formData.age}
                                    onChange={e => setFormData({ ...formData, age: e.target.value })}
                                />
                            </div>
                            <div className="space-y-2">
                                <label className="text-xs uppercase tracking-wider text-pastel-muted font-semibold">Gender *</label>
                                <select
                                    required
                                    className="w-full bg-white/5 border border-white/10 rounded-lg py-3.5 px-4 text-pastel-text text-base focus:outline-none focus:border-pastel-accent transition-colors appearance-none"
                                    value={formData.gender}
                                    onChange={e => setFormData({ ...formData, gender: e.target.value })}
                                >
                                    <option value="" disabled>Select</option>
                                    <option value="Female">Female</option>
                                    <option value="Male">Male</option>
                                </select>
                            </div>
                        </div>

                        <div className="space-y-2">
                            <label className="text-xs uppercase tracking-wider text-pastel-muted font-semibold">Email Address *</label>
                            <div className="relative">
                                <Mail className="absolute left-3 top-3.5 text-pastel-muted" size={18} />
                                <input
                                    type="email"
                                    required
                                    className="w-full bg-white/5 border border-white/10 rounded-lg py-3.5 pl-10 pr-4 text-pastel-text text-base focus:outline-none focus:border-pastel-accent transition-colors"
                                    placeholder="jane@example.com"
                                    value={formData.email}
                                    onChange={e => setFormData({ ...formData, email: e.target.value })}
                                />
                            </div>
                        </div>

                        <div className="space-y-2">
                            <label className="text-xs uppercase tracking-wider text-pastel-muted font-semibold">Phone Number *</label>
                            <div className="relative">
                                <Smartphone className="absolute left-3 top-3.5 text-pastel-muted" size={18} />
                                <input
                                    type="tel"
                                    required
                                    className="w-full bg-white/5 border border-white/10 rounded-lg py-3.5 pl-10 pr-4 text-pastel-text text-base focus:outline-none focus:border-pastel-accent transition-colors"
                                    placeholder="(555) 000-0000"
                                    value={formData.phone}
                                    onChange={e => setFormData({ ...formData, phone: e.target.value })}
                                />
                            </div>
                        </div>

                        <div className="space-y-2">
                            <label className="text-xs uppercase tracking-wider text-pastel-muted font-semibold">Zip Code *</label>
                            <input
                                type="text"
                                required
                                className="w-full bg-white/5 border border-white/10 rounded-lg py-3.5 px-4 text-pastel-text text-base focus:outline-none focus:border-pastel-accent transition-colors"
                                placeholder="10001"
                                value={formData.zip_code}
                                onChange={e => setFormData({ ...formData, zip_code: e.target.value })}
                            />
                        </div>

                        {/* Checkbox Removed as per request
                    <div className="flex items-start space-x-3 pt-2">
                         ...
                    </div> */}

                        {error && <p className="text-red-400 text-sm text-center">{error}</p>}

                        <button
                            type="submit"
                            disabled={loading}
                            className="w-full mt-6 bg-pastel-accent hover:bg-red-300 text-white font-bold py-4 text-lg rounded-lg transition-all transform hover:scale-[1.02] active:scale-[0.98] disabled:opacity-50 disabled:cursor-not-allowed touch-manipulation"
                        >
                            {loading ? "Saving..." : "Reveal My Results"}
                        </button>
                        <p className="text-xs text-center text-pastel-muted mt-4">
                            Model Scouters is not an agency
                        </p>
                    </form>
                </div>
            </div>
        </div>
    );
};

export default LeadForm;
