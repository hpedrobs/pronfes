import mongoose, { Document, Schema } from "mongoose"

mongoose.set('strictQuery', true)
mongoose.connect('mongodb://127.0.0.1:27017/nfe')
    .catch(error => console.error(error))

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
