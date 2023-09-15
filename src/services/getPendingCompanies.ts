import Pending from "../schemas/Pending";

export default async function () {
    const pendings = await Pending.aggregate([
        {
          '$group': {
            '_id': '$company_name', 
            'firstDocument': {
              '$first': '$$ROOT'
            }
          }
        }, {
          '$replaceRoot': {
            'newRoot': '$firstDocument'
          }
        }, {
          '$sort': {
            'company_name': 1
          }
        }
    ])

    pendings.forEach(company => console.log(`${company.company_name}`))
}
