package helper

import github.etx.test.Rand


val Rand.gender: Gender
    get() {
        val size = Gender.values().size
        return Gender.values()[int(0, size - 1)]
    }

val Rand.person: Person
    get() = Person(name = str, age = int, gender = gender)

