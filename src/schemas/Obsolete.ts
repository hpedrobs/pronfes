import mongoose, { Document, Schema } from "mongoose"

mongoose.set('strictQuery', true)
mongoose.connect('mongodb://127.0.0.1:27017/nfe')
    .catch(error => console.error(error))

export interface IObsolete extends Document {
    company: number;
}

const logSchema = new Schema<IObsolete>({
    company: { type: Number, required: true, unique: true }
})

export default mongoose.model<IObsolete>('Obsolete', logSchema);
