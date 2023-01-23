/* external */
import colors from 'colors'

const success = colors.green
const warn = colors.yellow
const error = colors.red
const info = colors.gray

function setColor (text:string, type:string) {
    switch (type) {
        case 'success':
            text = success(text)
            break
        case 'warn':
            text = warn(text)
            break
        case 'info':
            text = info(text)
            break
        case 'error':
            text = error(text)
            break
    }

    return text
}

export default function (text:string, type:string) {
    const name = colors.bgGreen.black('iacon-nfes')
    const time = colors.bold(new Date().toLocaleTimeString())
    text = setColor(text, type)
    console.log('>', name, time, text)
}