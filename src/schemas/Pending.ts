import mongoose, { Document, Schema } from "mongoose"
import '../config/mongoose/connect'

export interface IPending extends Document {
    company: number;
    company_name: string;
    filepath: string;
    period: string;
    active: boolean;
}

const pendingSchema = new Schema<IPending>({
    company: { type: Number, required: true },
    company_name: { type: String, required: false },
    filepath: { type: String, required: true },
    period: { type: String, required: true },
    active: { type: Boolean, required: true, default: false },
}, { timestamps: true })

export default mongoose.model<IPending>('Pending', pendingSchema);
