import Pending from "../schemas/Pending";
import { ObjectId } from "mongoose";

(async function () {
    const pendings = await Pending.find({ active: true })

    for (const { _id } of pendings) {
        console.log(`_id: ${_id}`)
        await Pending.updateOne({ _id }, { active: false })
    }
})()