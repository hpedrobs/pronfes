import mongoose, { Document, Schema } from "mongoose"
import '../config/mongoose/connect'

export interface IObsolete extends Document {
    company: number;
}

const logSchema = new Schema<IObsolete>({
    company: { type: Number, required: true, unique: true }
})

export default mongoose.model<IObsolete>('Obsolete', logSchema);
