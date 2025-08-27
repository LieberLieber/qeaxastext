
# QEAX as text

The purpose is to showcase how EA Models can be text in github and qeax(sqlite) in the local repo automatically.

Before cloning this Repo make sure to install [gitsqlite](https://github.com/danielsiegl/gitsqlite)!

`winget install danielsiegl.gitsqlite`

And set up the filters in your .gitconfig

```bash
git config --global filter.gitsqlite.clean "gitsqlite clean"
git config --global filter.gitsqlite.smudge "gitsqlite smudge"
```