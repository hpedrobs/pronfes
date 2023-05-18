import mongoose, { Document, Schema } from "mongoose"
import '../config/mongoose/connect'

export interface ILog extends Document {
    company: number;
    filepath: string;
    log: string;
    type: string;
}

const logSchema = new Schema<ILog>({
    company: { type: Number, required: true },
    filepath: { type: String, required: true },
    log: { type: String, required: true },
    type: { type: String, enum: ['success', 'error'], required: true }
}, { timestamps: true })

export default mongoose.model<ILog>('Log', logSchema);
