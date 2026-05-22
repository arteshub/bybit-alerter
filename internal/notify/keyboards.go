package notify

import (
	"fmt"

	"volume_pump_checker/domain/user"
)

var (
	multOptions = []float64{2, 3, 5, 7, 10, 15}
	daysOptions = []int{30, 60, 90}
)

type inlineButton struct {
	Text         string `json:"text"`
	CallbackData string `json:"callback_data"`
}

type inlineKeyboard struct {
	InlineKeyboard [][]inlineButton `json:"inline_keyboard"`
}

func settingsMenu(u *user.User) (string, inlineKeyboard) {
	text := fmt.Sprintf(
		"⚙️ <b>Настройки</b>\n\nМножитель: <b>x%.0f</b>\nПериод: <b>%d дн.</b>",
		u.VolumeMultiplier, u.LookbackDays,
	)
	kb := inlineKeyboard{InlineKeyboard: [][]inlineButton{
		{{Text: "📊 Множитель", CallbackData: "mult"}, {Text: "📅 Период", CallbackData: "days"}},
		{{Text: "🚫 Отписаться", CallbackData: "stop"}},
	}}
	return text, kb
}

func multMenu(current float64) (string, inlineKeyboard) {
	var rows [][]inlineButton
	var row []inlineButton
	for i, m := range multOptions {
		label := fmt.Sprintf("x%.0f", m)
		if m == current {
			label = "✅ " + label
		}
		row = append(row, inlineButton{Text: label, CallbackData: fmt.Sprintf("mult:%d", int(m))})
		if len(row) == 3 || i == len(multOptions)-1 {
			rows = append(rows, row)
			row = nil
		}
	}
	rows = append(rows, []inlineButton{{Text: "◀️ Назад", CallbackData: "settings"}})
	return "📊 <b>Множитель объёма</b>\n\nАлерт срабатывает когда оборот превышает среднее в N раз:", inlineKeyboard{InlineKeyboard: rows}
}

func daysMenu(current int) (string, inlineKeyboard) {
	var row []inlineButton
	for _, d := range daysOptions {
		label := fmt.Sprintf("%d дн.", d)
		if d == current {
			label = "✅ " + label
		}
		row = append(row, inlineButton{Text: label, CallbackData: fmt.Sprintf("days:%d", d)})
	}
	kb := inlineKeyboard{InlineKeyboard: [][]inlineButton{
		row,
		{{Text: "◀️ Назад", CallbackData: "settings"}},
	}}
	return "📅 <b>Период усреднения</b>\n\nСреднее считается за выбранное количество дней:", kb
}
