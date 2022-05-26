/* external */
import colors from 'colors'

const success = colors.green
const warn = colors.yellow
const error = colors.red
const info = colors.gray

function setColor (text, type) {
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

function setTitleDescription (text, type) {
    switch (type) {
    case 'warn':
        text = `warning: ${text}`
        break
    case 'error':
        text = `error: ${text}`
        break
    case 'info':
        text = `info: ${text}`
        break
    }

    return text
}

export default function (text, type = '') {
    const name = colors.bgGreen.black('NFE PROCESS')
    const time = colors.bold(new Date().toLocaleTimeString())
    text = setColor(setTitleDescription(text, type), type)
    console.log('>', name, time, text)
}
