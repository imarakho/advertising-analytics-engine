import js from "@eslint/js";
import importPlugin from "eslint-plugin-import";
import unusedImports from "eslint-plugin-unused-imports";
import tseslint from "typescript-eslint";

export default [
  js.configs.recommended,
  ...tseslint.configs.recommended,

  {
    files: ["**/*.ts"],
    languageOptions: {
      parserOptions: {
        project: true,
        tsconfigRootDir: import.meta.dirname
      }
    },
    plugins: {
      import: importPlugin,
      "unused-imports": unusedImports
    },
    rules: {
      "no-console": "off",
      "unused-imports/no-unused-imports": "error",
      "@typescript-eslint/no-unused-vars": [
        "warn",
        { argsIgnorePattern: "^_", varsIgnorePattern: "^_" }
      ],
      "import/order": "off"
    },
    settings: {
      "import/resolver": {
        typescript: true
      }
    }
  },

  {
    ignores: ["jest.config.cjs", "dist/**", "node_modules/**"]
  }
];
