import mongoose, { Document, Schema } from "mongoose"
import '../config/mongoose/connect'

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
