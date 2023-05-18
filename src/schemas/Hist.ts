import mongoose, { Document, Schema } from "mongoose"
import '../config/mongoose/connect'

export interface IHist extends Document {
    company: number;
    filepath: string;
    period: string;
}

const histSchema = new Schema<IHist>({
    company: { type: Number, required: true },
    filepath: { type: String, required: true },
    period: { type: String, required: true }
}, { timestamps: true })

export default mongoose.model<IHist>('Hist', histSchema);
