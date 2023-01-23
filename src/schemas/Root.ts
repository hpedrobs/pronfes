import mongoose, { Document, Schema } from "mongoose"

mongoose.set('strictQuery', true)
mongoose.connect('mongodb://127.0.0.1:27017/nfe')
    .catch(error => console.error(error))

export interface IRoot extends Document {
    pathname: string;
}

const rootSchema = new Schema<IRoot>({
    pathname: {
        type: String,
        required: true,
        unique: true
    }
})

export default mongoose.model<IRoot>('Root', rootSchema);
