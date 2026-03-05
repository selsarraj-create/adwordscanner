
import React from 'react';
import { Star, Quote } from 'lucide-react';

const testimonials = [
    {
        id: 1,
        name: "Sarah Jenkins",
        role: "Aspiring Model",
        quote: "I never thought I had model potential until I used this scanner. Two weeks later, I was signed!",
        image: "/t-sarah.png"
    },
    {
        id: 2,
        name: "Michael Chen",
        role: "Commercial Model",
        quote: "The analysis was spot on about my face shape. It gave me the confidence to apply to agencies.",
        image: "/t-michael.png"
    },
    {
        id: 3,
        name: "Jessica Williams",
        role: "Freelance Talent",
        quote: "Fast, accurate, and incredibly helpful. The agency finder tool is a game changer.",
        image: "/t-jessica.png"
    }
];

const Testimonials = () => {
    return (
        <div className="w-full max-w-6xl mx-auto px-4 py-12">
            <div className="text-center mb-10">
                <h2 className="text-2xl sm:text-3xl font-bold text-white mb-2">Success Stories</h2>
                <p className="text-pastel-muted">Real people who discovered their potential</p>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                {testimonials.map((testimonial) => (
                    <div key={testimonial.id} className="glass-panel p-6 rounded-xl border border-white/5 bg-white/5 relative group hover:bg-white/10 transition-colors">
                        <Quote className="absolute top-4 right-4 text-pastel-accent/20" size={40} />

                        <div className="flex items-center gap-4 mb-4">
                            <div className="w-16 h-16 rounded-full overflow-hidden border-2 border-pastel-accent/50">
                                {/* Using individual images */}
                                <img
                                    src={testimonial.image}
                                    alt={testimonial.name}
                                    className="w-full h-full object-cover"
                                />
                            </div>
                            <div>
                                <h3 className="font-bold text-white text-lg leading-tight">{testimonial.name}</h3>
                                <p className="text-xs text-pastel-accent uppercase tracking-wider">{testimonial.role}</p>
                            </div>
                        </div>

                        <div className="flex gap-1 mb-3">
                            {[...Array(5)].map((_, i) => (
                                <Star key={i} size={14} className="fill-pastel-accent text-pastel-accent" />
                            ))}
                        </div>

                        <p className="text-pastel-text text-sm leading-relaxed">
                            "{testimonial.quote}"
                        </p>
                    </div>
                ))}
            </div>
        </div>
    );
};

export default Testimonials;
