import mongoose, { Document, Schema } from "mongoose"

mongoose.set('strictQuery', true)
mongoose.connect('mongodb://127.0.0.1:27017/nfe')
    .catch(error => console.error(error))

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
